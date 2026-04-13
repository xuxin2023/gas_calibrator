from __future__ import annotations

import csv
import json
from pathlib import Path

from openpyxl import Workbook

from gas_calibrator.tools import audit_run


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_workbook(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(["ok"])
    wb.save(path)
    wb.close()


def _runtime_cfg() -> dict:
    return {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "enabled": True},
                {"name": "ga02", "enabled": True},
            ]
        },
        "workflow": {
            "sampling": {"count": 10, "stable_count": 10},
        },
        "paths": {"points_excel": "points.xlsx"},
    }


def _runtime_cfg_eight_analyzers() -> dict:
    return {
        "devices": {
            "gas_analyzers": [
                {"name": f"ga{idx:02d}", "enabled": True}
                for idx in range(1, 9)
            ]
        },
        "workflow": {
            "sampling": {"count": 10, "stable_count": 10},
        },
        "paths": {"points_excel": "points.xlsx"},
    }


def _make_sample_file(path: Path, rows: int) -> None:
    _write_csv(
        path,
        ["sample_ts", "value"],
        [{"sample_ts": idx, "value": idx} for idx in range(rows)],
    )


def _monkeypatch_artifacts(monkeypatch, run_dir: Path, *, suffix: str) -> None:
    artifact_map = {
        "io_*.csv": run_dir / f"io_{suffix}.csv",
        "points_*.csv": run_dir / f"points_{suffix}.csv",
        "points_readable_*.csv": run_dir / f"points_readable_{suffix}.csv",
        "points_readable_*.xlsx": run_dir / f"points_readable_{suffix}.xlsx",
        "分析仪汇总_*.csv": run_dir / f"analyzer_summary_{suffix}.csv",
        "分析仪汇总_*.xlsx": run_dir / f"analyzer_summary_{suffix}.xlsx",
    }
    monkeypatch.setattr(audit_run, "_latest_artifact", lambda _run_dir, pattern: artifact_map.get(pattern))


def test_audit_run_dir_passes_complete_run(monkeypatch, tmp_path: Path) -> None:
    suffix = "20260314_120000"
    run_dir = tmp_path / f"run_{suffix}"
    run_dir.mkdir()
    runtime_cfg = _runtime_cfg()
    (run_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    planned = [
        audit_run.PlannedPointSpec(3, "co2", "co2_groupa_0ppm_1100hpa"),
        audit_run.PlannedPointSpec(7, "h2o", "h2o_20c_50rh_1100hpa"),
    ]
    monkeypatch.setattr(audit_run, "plan_points_from_runtime_config", lambda cfg: list(planned))
    _monkeypatch_artifacts(monkeypatch, run_dir, suffix=suffix)

    _write_csv(
        run_dir / f"io_{suffix}.csv",
        ["timestamp", "port", "device", "direction", "command", "response", "error"],
        [
            {
                "timestamp": "2026-03-14T12:00:00",
                "port": "RUN",
                "device": "runner",
                "direction": "EVENT",
                "command": "run-start",
                "response": "",
                "error": "",
            },
            {
                "timestamp": "2026-03-14T12:10:00",
                "port": "RUN",
                "device": "runner",
                "direction": "EVENT",
                "command": "run-finished",
                "response": "",
                "error": "",
            },
        ],
    )
    _write_csv(
        run_dir / f"points_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
        ],
    )
    _write_csv(
        run_dir / f"points_readable_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
        ],
    )
    _write_workbook(run_dir / f"points_readable_{suffix}.xlsx")
    _write_csv(
        run_dir / f"analyzer_summary_{suffix}.csv",
        ["Analyzer"],
        [
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
        ],
    )
    _write_workbook(run_dir / f"analyzer_summary_{suffix}.xlsx")

    for spec in planned:
        _make_sample_file(run_dir / spec.sample_filename, rows=10)

    result = audit_run.audit_run_dir(run_dir)

    assert result.ok
    assert not result.failures


def test_audit_run_dir_prefers_expected_analyzers_from_point_exports(
    monkeypatch,
    tmp_path: Path,
) -> None:
    suffix = "20260314_121000"
    run_dir = tmp_path / f"run_{suffix}"
    run_dir.mkdir()
    runtime_cfg = _runtime_cfg_eight_analyzers()
    (run_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    planned = [
        audit_run.PlannedPointSpec(3, "co2", "co2_groupa_0ppm_ambient"),
        audit_run.PlannedPointSpec(7, "h2o", "h2o_20c_50rh_ambient"),
    ]
    monkeypatch.setattr(audit_run, "plan_points_from_runtime_config", lambda cfg: list(planned))
    _monkeypatch_artifacts(monkeypatch, run_dir, suffix=suffix)

    _write_csv(
        run_dir / f"io_{suffix}.csv",
        ["timestamp", "port", "device", "direction", "command", "response", "error"],
        [
            {
                "timestamp": "2026-03-14T12:10:00",
                "port": "RUN",
                "device": "runner",
                "direction": "EVENT",
                "command": "run-finished",
                "response": "",
                "error": "",
            },
        ],
    )
    point_rows = [
        {
            audit_run.POINT_INTEGRITY_LABEL: "完整",
            audit_run.EXPECTED_ANALYZERS_LABEL: 4,
        },
        {
            audit_run.POINT_INTEGRITY_LABEL: "完整",
            audit_run.EXPECTED_ANALYZERS_LABEL: 4,
        },
    ]
    _write_csv(
        run_dir / f"points_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL, audit_run.EXPECTED_ANALYZERS_LABEL],
        point_rows,
    )
    _write_csv(
        run_dir / f"points_readable_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL, audit_run.EXPECTED_ANALYZERS_LABEL],
        point_rows,
    )
    _write_workbook(run_dir / f"points_readable_{suffix}.xlsx")
    _write_csv(
        run_dir / f"analyzer_summary_{suffix}.csv",
        ["Analyzer"],
        [
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
            {"Analyzer": "GA03"},
            {"Analyzer": "GA04"},
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
            {"Analyzer": "GA03"},
            {"Analyzer": "GA04"},
        ],
    )
    _write_workbook(run_dir / f"analyzer_summary_{suffix}.xlsx")

    for spec in planned:
        _make_sample_file(run_dir / spec.sample_filename, rows=10)

    result = audit_run.audit_run_dir(run_dir)

    assert result.ok
    assert any("Expected analyzers per point: 4 (point_exports)" in item for item in result.infos)


def test_audit_run_dir_flags_missing_outputs(monkeypatch, tmp_path: Path) -> None:
    suffix = "20260314_120500"
    run_dir = tmp_path / f"run_{suffix}"
    run_dir.mkdir()
    runtime_cfg = _runtime_cfg()
    (run_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    planned = [
        audit_run.PlannedPointSpec(3, "co2", "co2_groupa_0ppm_1100hpa"),
        audit_run.PlannedPointSpec(7, "h2o", "h2o_20c_50rh_1100hpa"),
    ]
    monkeypatch.setattr(audit_run, "plan_points_from_runtime_config", lambda cfg: list(planned))
    _monkeypatch_artifacts(monkeypatch, run_dir, suffix=suffix)

    _write_csv(
        run_dir / f"io_{suffix}.csv",
        ["timestamp", "port", "device", "direction", "command", "response", "error"],
        [
            {
                "timestamp": "2026-03-14T12:05:00",
                "port": "RUN",
                "device": "runner",
                "direction": "EVENT",
                "command": "run-aborted",
                "response": "",
                "error": "boom",
            }
        ],
    )
    _write_csv(
        run_dir / f"points_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "部分"},
        ],
    )
    _write_csv(
        run_dir / f"points_readable_{suffix}.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "部分"},
        ],
    )
    _write_workbook(run_dir / f"points_readable_{suffix}.xlsx")
    _write_csv(
        run_dir / f"analyzer_summary_{suffix}.csv",
        ["Analyzer"],
        [
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
        ],
    )
    _write_workbook(run_dir / f"analyzer_summary_{suffix}.xlsx")

    _make_sample_file(run_dir / planned[0].sample_filename, rows=3)

    result = audit_run.audit_run_dir(run_dir)

    assert not result.ok
    assert any("run-aborted" in item for item in result.failures)
    assert any("Missing sample file" in item for item in result.failures)
    assert any("integrity is not complete" in item for item in result.failures)
