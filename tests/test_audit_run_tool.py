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


def _make_sample_file(path: Path, rows: int) -> None:
    _write_csv(
        path,
        ["sample_ts", "value"],
        [{"sample_ts": idx, "value": idx} for idx in range(rows)],
    )


def test_audit_run_dir_passes_complete_run(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "run_20260314_120000"
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

    _write_csv(
        run_dir / "io_20260314_120000.csv",
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
        run_dir / "points_20260314_120000.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
        ],
    )
    _write_csv(
        run_dir / "points_readable_20260314_120000.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
        ],
    )
    _write_workbook(run_dir / "points_readable_20260314_120000.xlsx")
    _write_csv(
        run_dir / "分析仪汇总_20260314_120000.csv",
        ["Analyzer"],
        [
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
        ],
    )
    _write_workbook(run_dir / "分析仪汇总_20260314_120000.xlsx")

    for spec in planned:
        _make_sample_file(run_dir / spec.sample_filename, rows=10)

    result = audit_run.audit_run_dir(run_dir)

    assert result.ok
    assert not result.failures


def test_audit_run_dir_flags_missing_outputs(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "run_20260314_120500"
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

    _write_csv(
        run_dir / "io_20260314_120500.csv",
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
        run_dir / "points_20260314_120500.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "部分"},
        ],
    )
    _write_csv(
        run_dir / "points_readable_20260314_120500.csv",
        [audit_run.POINT_INTEGRITY_LABEL],
        [
            {audit_run.POINT_INTEGRITY_LABEL: "完整"},
            {audit_run.POINT_INTEGRITY_LABEL: "部分"},
        ],
    )
    _write_workbook(run_dir / "points_readable_20260314_120500.xlsx")
    _write_csv(
        run_dir / "分析仪汇总_20260314_120500.csv",
        ["Analyzer"],
        [
            {"Analyzer": "GA01"},
            {"Analyzer": "GA02"},
        ],
    )
    _write_workbook(run_dir / "分析仪汇总_20260314_120500.xlsx")

    _make_sample_file(run_dir / planned[0].sample_filename, rows=3)

    result = audit_run.audit_run_dir(run_dir)

    assert not result.ok
    assert any("run-aborted" in item for item in result.failures)
    assert any("Missing sample file" in item for item in result.failures)
    assert any("integrity is not complete" in item for item in result.failures)
