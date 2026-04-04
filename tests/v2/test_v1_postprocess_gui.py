from __future__ import annotations

from pathlib import Path

import pandas as pd

from gas_calibrator.v2.scripts.v1_postprocess_gui import (
    PostprocessJobOptions,
    build_coefficient_preview,
    run_postprocess_job,
)


def test_run_postprocess_job_passes_expected_arguments() -> None:
    captured: dict[str, object] = {}

    def fake_runner(**kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        return {"report": "report.xlsx", "summary": "summary.json"}

    result = run_postprocess_job(
        PostprocessJobOptions(
            run_dir=r"D:\logs\run_20260319_120000",
            config_path=r"D:\gas_calibrator\configs\default_config.json",
            output_dir=r"D:\logs\run_20260319_120000",
            download=True,
        ),
        runner=fake_runner,
    )

    assert result["report"] == "report.xlsx"
    assert captured == {
        "run_dir": r"D:\logs\run_20260319_120000",
        "config_path": r"D:\gas_calibrator\configs\default_config.json",
        "output_dir": r"D:\logs\run_20260319_120000",
        "download": True,
    }


def test_run_postprocess_job_requires_run_dir_and_config() -> None:
    try:
        run_postprocess_job(PostprocessJobOptions(run_dir="", config_path="cfg.json"))
    except ValueError as exc:
        assert str(exc) == "run_dir is required"
    else:  # pragma: no cover
        raise AssertionError("expected run_dir validation error")

    try:
        run_postprocess_job(PostprocessJobOptions(run_dir="run", config_path=""))
    except ValueError as exc:
        assert str(exc) == "config_path is required"
    else:  # pragma: no cover
        raise AssertionError("expected config_path validation error")


def test_build_coefficient_preview_formats_scientific_notation(tmp_path: Path) -> None:
    report_path = tmp_path / "calibration_coefficients.xlsx"
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "分析仪": "GA01",
                    "气体": "CO2",
                    "数据范围": "0-2000",
                    "a0": 238701.8692947513,
                    "a1": -543907.9681802709,
                }
            ]
        ).to_excel(writer, sheet_name="简化系数", index=False)

    preview = build_coefficient_preview(report_path)

    assert "GA01" in preview
    assert "CO2" in preview
    assert "2.38702E+05" in preview
    assert "-5.43908E+05" in preview
