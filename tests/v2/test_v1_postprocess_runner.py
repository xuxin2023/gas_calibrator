from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import func, select

from gas_calibrator.v2.adapters import v1_postprocess_runner
from gas_calibrator.v2.exceptions import ConfigurationInvalidError
from gas_calibrator.v2.storage.database import DatabaseManager, StorageSettings, resolve_run_uuid
from gas_calibrator.v2.storage.models import FitResultRecord, PointRecord, QCResultRecord, RunRecord, SampleRecord


def _build_summary_workbook(path: Path) -> None:
    rows: list[dict[str, object]] = []
    for idx, target in enumerate([0.0, 200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0], start=1):
        temp = 20.0 + float(idx % 3)
        pressure = 90.0 + float((idx % 4) * 5.0)
        rows.append(
            {
                "Analyzer": "GA01",
                "NUM": idx,
                "PointRow": idx,
                "PointPhase": "姘旇矾",
                "PointTag": f"co2_{idx}",
                "PointTitle": f"姘旇矾 {temp}C CO2={target}ppm",
                "Temp": temp,
                "ppm_CO2_Tank": target,
                "ppm_H2O_Dew": 2.0 + idx * 0.1,
                "R_CO2": 1.0 + 0.0006 * target + 0.002 * temp + 0.0003 * pressure,
                "R_H2O": 0.2 + 0.0001 * idx,
                "BAR": pressure,
            }
        )
    for idx in range(10, 19):
        temp = 10.0 + float(idx % 4)
        pressure = 88.0 + float((idx % 3) * 4.0)
        dew_target = 4.0 + idx * 0.6
        rows.append(
            {
                "Analyzer": "GA01",
                "NUM": idx,
                "PointRow": idx,
                "PointPhase": "姘磋矾",
                "PointTag": f"h2o_{idx}",
                "PointTitle": f"姘磋矾 {temp}C RH={25 + idx}%",
                "Temp": temp,
                "ppm_CO2_Tank": 0.0,
                "ppm_H2O_Dew": dew_target,
                "R_CO2": 1.0 + 0.0001 * idx,
                "R_H2O": 0.5 + 0.015 * idx + 0.002 * temp + 0.0002 * pressure,
                "BAR": pressure,
            }
        )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="GA01", index=False)


def _write_run_artifacts(base_dir: Path) -> Path:
    run_dir = base_dir / "run_20260319_120000"
    run_dir.mkdir(parents=True, exist_ok=True)

    workbook_path = run_dir / "鍒嗘瀽浠眹鎬籣20260319_120000.xlsx"
    _build_summary_workbook(workbook_path)

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "run_20260319_120000",
                "generated_at": "2026-03-19T12:10:00+00:00",
                "status": {
                    "phase": "completed",
                    "total_points": 1,
                    "completed_points": 1,
                    "progress": 1.0,
                    "elapsed_s": 120.0,
                },
                "stats": {
                    "warning_count": 0,
                    "error_count": 0,
                    "enabled_devices": ["ga01", "temperature_chamber"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    samples = []
    samples_csv_rows = [
        "timestamp,point_index,temperature_c,co2_ppm,humidity_pct,pressure_hpa,route,analyzer_id,analyzer_serial,sample_index,sample_co2_ppm,sample_h2o_mmol,co2_ratio_f,h2o_ratio_f,co2_ratio_raw,h2o_ratio_raw,chamber_temp_c,case_temp_c,dewpoint_c"
    ]
    for sample_index in range(1, 7):
        timestamp = f"2026-03-19T12:08:0{sample_index}+00:00"
        samples.append(
            {
                "point": {
                    "index": 1,
                    "temperature_c": 25.0,
                    "co2_ppm": 400.0,
                    "pressure_hpa": 1000.0,
                    "route": "co2",
                },
                "analyzer_id": "ga01",
                "timestamp": timestamp,
                "co2_ppm": 400.0 + sample_index * 0.1,
                "h2o_mmol": 0.2,
                "co2_signal": 4000.0 + sample_index,
                "h2o_signal": 100.0 + sample_index,
                "co2_ratio_f": 1.001 + sample_index * 0.0001,
                "co2_ratio_raw": 1.001 + sample_index * 0.0001,
                "h2o_ratio_f": 0.201,
                "h2o_ratio_raw": 0.201,
                "temperature_c": 25.1,
                "pressure_hpa": 1000.0,
                "analyzer_chamber_temp_c": 25.2,
                "case_temp_c": 26.0,
            }
        )
        samples_csv_rows.append(
            f"{timestamp},1,25.0,400.0,,1000.0,co2,ga01,SN01,{sample_index},{400.0 + sample_index * 0.1},0.2,"
            f"{1.001 + sample_index * 0.0001},0.201,{1.001 + sample_index * 0.0001},0.201,25.2,26.0,4.0"
        )

    (run_dir / "results.json").write_text(
        json.dumps(
            {
                "run_id": "run_20260319_120000",
                "samples": samples,
                "point_summaries": [],
                "fit_results": [
                    {
                        "analyzer_id": "ga01",
                        "algorithm": "linear",
                        "coefficients": {"slope": 1.05, "intercept": 0.1},
                        "rmse": 0.02,
                        "r_squared": 0.998,
                        "n_points": 1,
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    (run_dir / "points.csv").write_text(
        "\n".join(
            [
                "timestamp,point_index,point_tag,temperature_c,co2_ppm,humidity_pct,pressure_hpa,route,status,stability_time_s,total_time_s",
                "2026-03-19T12:08:00+00:00,1,co2_1,25.0,400.0,,1000.0,co2,completed,10.0,30.0",
            ]
        ),
        encoding="utf-8",
    )

    (run_dir / "samples.csv").write_text("\n".join(samples_csv_rows), encoding="utf-8")
    runtime_rows = [
        "point_index,point_phase,point_tag,sample_index,sample_ts,temp_set_c,pressure_target_hpa,co2_ppm_target,h2o_mmol_target,point_is_h2o,"
        "ga01_frame_has_data,ga01_frame_usable,ga01_co2_ppm,ga01_h2o_mmol,ga01_co2_ratio_f,ga01_h2o_ratio_f,ga01_raw,ga01_id,ga01_mode,ga01_mode2_field_count,"
        "ga01_co2_ratio_raw,ga01_h2o_ratio_raw,ga01_ref_signal,ga01_co2_signal,ga01_h2o_signal,ga01_chamber_temp_c,ga01_case_temp_c,ga01_pressure_kpa,ga01_status,"
        "pressure_hpa,dewpoint_c,chamber_temp_c,chamber_rh_pct,hgen_Uw,analyzer_expected_count,analyzer_with_frame_count,analyzer_usable_count,analyzer_coverage_text,analyzer_integrity,analyzer_missing_labels,analyzer_unusable_labels,stability_time_s,total_time_s"
    ]
    for sample_index in range(1, 7):
        frame_has_data = "False" if sample_index == 6 else "True"
        frame_usable = "False" if sample_index == 6 else "True"
        status = "fault" if sample_index == 6 else ""
        coverage_text = "0/1" if sample_index == 6 else "1/1"
        integrity = "partial" if sample_index == 6 else "complete"
        missing_labels = "ga02" if sample_index == 6 else ""
        unusable_labels = "ga01" if sample_index == 6 else ""
        runtime_rows.append(
            "1,completed,co2_1,"
            f"{sample_index},2026-03-19T12:08:0{sample_index}+00:00,25.0,1000.0,400.0,,False,"
            f"{frame_has_data},{frame_usable},{400.0 + sample_index * 0.1},0.2,{1.001 + sample_index * 0.0001},0.201,"
            f"FRAME{sample_index},010,2,16,{1.001 + sample_index * 0.0001},0.201,{3500.0 + sample_index},{4000.0 + sample_index},"
            f"{100.0 + sample_index},25.2,26.0,100.0,{status},1000.0,4.0,25.0,45.0,52.7,1,1,{0 if sample_index == 6 else 1},"
            f"{coverage_text},{integrity},{missing_labels},{unusable_labels},10.0,30.0"
        )
    (run_dir / "samples_runtime.csv").write_text("\n".join(runtime_rows), encoding="utf-8")
    (run_dir / "io_log.csv").write_text(
        "\n".join(
            [
                "timestamp,device,direction,data",
                "2026-03-19T12:08:00+00:00,ga01,tx,READ",
                "2026-03-19T12:08:00+00:00,ga01,rx,OK",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "run.log").write_text(
        json.dumps({"timestamp": "2026-03-19T12:08:00+00:00", "level": "INFO", "message": "run start", "context": {}}),
        encoding="utf-8",
    )

    return run_dir


def _assert_analytics_bundle(analytics_dir: Path) -> None:
    expected = {
        "run_kpis.json",
        "point_kpis.json",
        "drift_report.json",
        "analyzer_health.json",
        "fault_attribution.json",
        "coefficient_lineage.json",
    }
    assert analytics_dir.exists()
    assert expected <= {path.name for path in analytics_dir.glob("*.json")}


def _assert_measurement_analytics_bundle(analytics_dir: Path) -> None:
    expected = {
        "measurement_quality.json",
        "measurement_drift_report.json",
        "signal_anomalies.json",
        "context_attribution.json",
        "instrument_health.json",
    }
    assert analytics_dir.exists()
    assert expected <= {path.name for path in analytics_dir.glob("*.json")}


def _build_split_summary_workbooks(run_dir: Path) -> tuple[Path, Path]:
    gas_path = run_dir / "\u5206\u6790\u4eea\u6c47\u603b_\u6c14\u8def_20260319_120000.xlsx"
    water_path = run_dir / "\u5206\u6790\u4eea\u6c47\u603b_\u6c34\u8def_20260319_120000.xlsx"
    _build_summary_workbook(gas_path)
    _build_summary_workbook(water_path)
    return gas_path, water_path


def _summary_paths(run_dir: Path) -> list[str]:
    return [str(next(run_dir.glob("*.xlsx")).resolve())]


def _patch_report_and_refit(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_report_step(*, summary_paths: list[Path], target_dir: Path, coeff_cfg: object) -> dict[str, str]:
        output_path = target_dir / "calibration_coefficients.xlsx"
        output_path.write_text("offline report", encoding="utf-8")
        return {"status": "completed", "path": str(output_path)}

    def fake_refit_step(
        *,
        summary_paths: list[Path],
        config_path: str | None,
        target_dir: Path,
        skip_refit: bool,
    ) -> dict[str, object]:
        if skip_refit:
            return {"status": "skipped", "reason": "skip_refit enabled"}
        runs = []
        for gas in ("co2", "h2o"):
            gas_dir = target_dir / "offline_refit" / "GA01" / gas
            gas_dir.mkdir(parents=True, exist_ok=True)
            excel = gas_dir / "refit.xlsx"
            audit_csv = gas_dir / "audit.csv"
            summary_csv = gas_dir / "summary.csv"
            excel.write_text("excel", encoding="utf-8")
            audit_csv.write_text("audit", encoding="utf-8")
            summary_csv.write_text("summary", encoding="utf-8")
            runs.append(
                {
                    "analyzer": "GA01",
                    "gas": gas,
                    "status": "completed",
                    "excel": str(excel),
                    "audit_csv": str(audit_csv),
                    "summary_csv": str(summary_csv),
                }
            )
        return {"status": "completed", "runs": runs}

    monkeypatch.setattr(v1_postprocess_runner, "_report_step", fake_report_step)
    monkeypatch.setattr(v1_postprocess_runner, "_refit_step", fake_refit_step)


def test_v1_postprocess_runner_runs_full_offline_pipeline_without_import_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    def _unexpected_download(**kwargs: object) -> dict[str, str]:  # pragma: no cover - should never run
        raise AssertionError("download must stay disabled by default")

    monkeypatch.setattr(v1_postprocess_runner, "download_coefficients_to_analyzers", _unexpected_download)

    exported = v1_postprocess_runner.run_from_cli(run_dir=str(run_dir), summary_paths=_summary_paths(run_dir))

    assert Path(exported["report"]).exists()
    assert Path(exported["summary"]).exists()
    assert Path(exported["manifest"]).exists()
    assert Path(exported["qc_json"]).exists()
    assert Path(exported["qc_csv"]).exists()
    assert Path(exported["ai_run_summary"]).exists()
    assert Path(exported["analytics_dir"]).exists()
    assert Path(exported["measurement_analytics_dir"]).exists()
    _assert_analytics_bundle(Path(exported["analytics_dir"]))
    _assert_measurement_analytics_bundle(Path(exported["measurement_analytics_dir"]))

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["manifest"]["status"] == "completed"
    assert summary_payload["database_import"]["status"] == "skipped"
    assert summary_payload["qc"]["status"] == "completed"
    assert summary_payload["refit"]["status"] == "completed"
    assert summary_payload["ai"]["status"] == "completed"
    assert summary_payload["analytics"]["status"] == "completed"
    assert summary_payload["measurement_analytics"]["status"] == "completed"
    assert summary_payload["ai"]["client_mode"] == "mock"
    assert summary_payload["download"]["status"] == "skipped"
    assert len(exported["refit_runs"]) == 2
    assert all(item["status"] == "completed" for item in exported["refit_runs"])
    assert all(Path(item["excel"]).exists() for item in exported["refit_runs"])
    ai_text = Path(exported["ai_run_summary"]).read_text(encoding="utf-8")
    assert "# AI Run Summary" in ai_text
    assert "整体质量评分" in ai_text


def test_v1_postprocess_runner_triggers_download_when_requested(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {"name": "ga01", "enabled": True, "port": "COM35", "baud": 115200, "device_id": "001"}
                    ]
                },
                "coefficients": {
                    "enabled": True,
                    "auto_fit": True,
                    "model": "ratio_poly_rt_p",
                    "summary_columns": {
                        "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
                        "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    called: dict[str, object] = {}

    def fake_download(**kwargs: object) -> dict[str, str]:
        called.update(kwargs)
        return {
            "download_summary": str(tmp_path / "coefficient_download_summary.json"),
            "io_log": str(tmp_path / "coefficient_download_io.csv"),
        }

    monkeypatch.setattr(v1_postprocess_runner, "download_coefficients_to_analyzers", fake_download)

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        config_path=str(config_path),
        download=True,
    )

    assert Path(exported["report"]).exists()
    assert exported["download_summary"].endswith("coefficient_download_summary.json")
    assert str(called["config_path"]) == str(config_path)


def test_v1_postprocess_runner_reports_clear_errors(tmp_path: Path) -> None:
    missing = tmp_path / "missing_run"

    with pytest.raises(ConfigurationInvalidError, match="run_dir"):
        v1_postprocess_runner.run_from_cli(run_dir=str(missing))


def test_resolve_summary_paths_prefers_split_water_and_gas_workbooks(tmp_path: Path) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    gas_path, water_path = _build_split_summary_workbooks(run_dir)

    resolved = v1_postprocess_runner._resolve_summary_paths(run_dir=run_dir, summary_paths=None)

    assert resolved == [gas_path.resolve(), water_path.resolve()]


def test_resolve_summary_paths_falls_back_to_combined_workbook(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_20260319_120000"
    run_dir.mkdir(parents=True, exist_ok=True)
    combined_path = run_dir / "\u5206\u6790\u4eea\u6c47\u603b_20260319_120000.xlsx"
    _build_summary_workbook(combined_path)

    resolved = v1_postprocess_runner._resolve_summary_paths(run_dir=run_dir, summary_paths=None)

    assert len(resolved) == 1
    assert resolved[0] == combined_path.resolve()


def test_v1_postprocess_runner_can_optionally_import_into_sqlite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)
    db_path = tmp_path / "storage.sqlite"
    dsn = f"sqlite:///{db_path.as_posix()}"

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        import_db=True,
        dsn=dsn,
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["database_import"]["status"] == "completed"
    assert summary_payload["database_import"]["raw"]["status"] == "completed"
    assert summary_payload["database_import"]["enrich"]["status"] == "completed"
    assert summary_payload["analytics"]["status"] == "completed"
    assert summary_payload["analytics"]["source_mode"] == "configured_database"
    assert summary_payload["measurement_analytics"]["status"] == "completed"
    assert summary_payload["measurement_analytics"]["source_mode"] == "configured_database"
    _assert_analytics_bundle(Path(exported["analytics_dir"]))
    _assert_measurement_analytics_bundle(Path(exported["measurement_analytics_dir"]))

    database = DatabaseManager(StorageSettings(backend="sqlite", database=str(db_path)))
    try:
        with database.session_scope() as session:
            run_record = session.get(RunRecord, resolve_run_uuid("run_20260319_120000"))
            assert run_record is not None
            notes = json.loads(run_record.notes or "{}")
            assert session.execute(select(func.count(PointRecord.id))).scalar_one() == 1
            assert session.execute(select(func.count(SampleRecord.id))).scalar_one() == 6
            assert session.execute(select(func.count(QCResultRecord.id))).scalar_one() >= 1
            assert session.execute(select(func.count(FitResultRecord.id))).scalar_one() == 1
            assert notes["enrich"]["postprocess_summary_metadata"]["status"] == "loaded"
            assert notes["enrich"]["ai_summary_metadata"]["status"] == "completed"
    finally:
        database.dispose()


def test_v1_postprocess_runner_uses_mock_fallback_when_openai_key_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)
    config_path = tmp_path / "ai_config.json"
    config_path.write_text(
        json.dumps(
            {
                "coefficients": {
                    "enabled": True,
                    "auto_fit": True,
                    "model": "ratio_poly_rt_p",
                    "summary_columns": {
                        "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
                        "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
                    },
                },
                "ai": {
                    "enabled": True,
                    "provider": "openai",
                    "model": "gpt-4o-mini",
                    "api_key": "",
                    "fallback_to_mock": True,
                    "features": {
                        "run_summary": True,
                        "anomaly_diagnosis": True,
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        config_path=str(config_path),
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["ai"]["status"] == "completed"
    assert summary_payload["ai"]["provider"] == "openai"
    assert summary_payload["ai"]["client_mode"] == "mock"
    assert Path(exported["ai_run_summary"]).exists()


def test_v1_postprocess_runner_ai_failure_does_not_break_postprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    class BrokenSummarizer:
        def summarize_run_directory(self, run_dir: str, *, anomaly_diagnosis: str = "") -> str:
            raise RuntimeError("simulated ai failure")

    class QuietAdvisor:
        def diagnose_run(self, **kwargs: object) -> str:
            return "diagnosis"

    fake_runtime = SimpleNamespace(
        llm=SimpleNamespace(config=SimpleNamespace(provider="mock")),
        summarizer=BrokenSummarizer(),
        anomaly_advisor=QuietAdvisor(),
    )

    monkeypatch.setattr(
        v1_postprocess_runner.AIRuntime,
        "from_config",
        classmethod(lambda cls, config: fake_runtime),
    )

    exported = v1_postprocess_runner.run_from_cli(run_dir=str(run_dir), summary_paths=_summary_paths(run_dir))

    assert Path(exported["report"]).exists()
    assert Path(exported["summary"]).exists()
    assert Path(exported["ai_run_summary"]).exists()
    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["ai"]["status"] == "fallback"
    assert "simulated ai failure" in summary_payload["ai"]["error"]
    assert "已退化为本地说明" in Path(exported["ai_run_summary"]).read_text(encoding="utf-8")
    assert summary_payload["analytics"]["status"] == "completed"


def test_v1_postprocess_runner_analytics_failure_does_not_break_postprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)
    original_render_report = v1_postprocess_runner.AnalyticsService.render_report

    def broken_render_report(
        self,
        report_name: str,
        *,
        features: dict[str, object],
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ):
        if report_name == "fault_attribution":
            raise RuntimeError("simulated analytics failure")
        return original_render_report(self, report_name, features=features, run_id=run_id, analyzer_id=analyzer_id)

    monkeypatch.setattr(v1_postprocess_runner.AnalyticsService, "render_report", broken_render_report)

    exported = v1_postprocess_runner.run_from_cli(run_dir=str(run_dir), summary_paths=_summary_paths(run_dir))

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["qc"]["status"] == "completed"
    assert summary_payload["refit"]["status"] == "completed"
    assert summary_payload["ai"]["status"] == "completed"
    assert summary_payload["analytics"]["status"] == "partial"
    assert summary_payload["analytics"]["failures"]["fault_attribution"]["status"] == "failed"
    assert Path(summary_payload["analytics"]["reports"]["run_kpis"]["path"]).exists()
    assert Path(summary_payload["analytics"]["reports"]["analyzer_health"]["path"]).exists()


def test_v1_postprocess_runner_can_skip_analytics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        skip_analytics=True,
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["analytics"]["status"] == "skipped"
    assert "analytics_dir" not in exported
    assert not (run_dir / "analytics").exists()
    assert summary_payload["measurement_analytics"]["status"] == "completed"
    _assert_measurement_analytics_bundle(Path(exported["measurement_analytics_dir"]))


def test_v1_postprocess_runner_supports_analytics_only_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        analytics_only=True,
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["flags"]["analytics_only"] is True
    assert summary_payload["qc"]["status"] == "skipped"
    assert summary_payload["report"]["status"] == "skipped"
    assert summary_payload["refit"]["status"] == "skipped"
    assert summary_payload["ai"]["status"] == "skipped"
    assert summary_payload["analytics"]["status"] == "completed"
    assert summary_payload["measurement_analytics"]["status"] == "completed"
    assert summary_payload["download"]["status"] == "skipped"
    assert "report" not in exported
    _assert_analytics_bundle(Path(exported["analytics_dir"]))
    _assert_measurement_analytics_bundle(Path(exported["measurement_analytics_dir"]))


def test_v1_postprocess_runner_measurement_analytics_failure_does_not_break_postprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)
    original_render_report = v1_postprocess_runner.MeasurementAnalyticsService.render_report

    def broken_render_report(
        self,
        report_name: str,
        *,
        features: dict[str, object],
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ):
        if report_name == "signal_anomaly":
            raise RuntimeError("simulated measurement analytics failure")
        return original_render_report(self, report_name, features=features, run_id=run_id, analyzer_id=analyzer_id)

    monkeypatch.setattr(v1_postprocess_runner.MeasurementAnalyticsService, "render_report", broken_render_report)

    exported = v1_postprocess_runner.run_from_cli(run_dir=str(run_dir), summary_paths=_summary_paths(run_dir))

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["qc"]["status"] == "completed"
    assert summary_payload["analytics"]["status"] == "completed"
    assert summary_payload["measurement_analytics"]["status"] == "partial"
    assert summary_payload["measurement_analytics"]["failures"]["signal_anomalies"]["status"] == "failed"
    assert Path(summary_payload["measurement_analytics"]["reports"]["measurement_quality"]["path"]).exists()
    assert Path(summary_payload["measurement_analytics"]["reports"]["instrument_health"]["path"]).exists()


def test_v1_postprocess_runner_can_skip_measurement_analytics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        skip_measurement_analytics=True,
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["measurement_analytics"]["status"] == "skipped"
    assert "measurement_analytics_dir" not in exported
    assert not (run_dir / "measurement_analytics").exists()


def test_v1_postprocess_runner_supports_measurement_analytics_only_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = _write_run_artifacts(tmp_path)
    _patch_report_and_refit(monkeypatch)

    exported = v1_postprocess_runner.run_from_cli(
        run_dir=str(run_dir),
        summary_paths=_summary_paths(run_dir),
        measurement_analytics_only=True,
    )

    summary_payload = json.loads(Path(exported["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["flags"]["measurement_analytics_only"] is True
    assert summary_payload["qc"]["status"] == "skipped"
    assert summary_payload["report"]["status"] == "skipped"
    assert summary_payload["refit"]["status"] == "skipped"
    assert summary_payload["ai"]["status"] == "skipped"
    assert summary_payload["analytics"]["status"] == "skipped"
    assert summary_payload["measurement_analytics"]["status"] == "completed"
    assert summary_payload["download"]["status"] == "skipped"
    assert "report" not in exported
    assert "analytics_dir" not in exported
    _assert_measurement_analytics_bundle(Path(exported["measurement_analytics_dir"]))
