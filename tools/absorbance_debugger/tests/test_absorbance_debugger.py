from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from tools.absorbance_debugger.app import run_debugger
from tools.absorbance_debugger.io.run_bundle import RunBundle, discover_run_artifacts
from tools.absorbance_debugger.options import (
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_RUN_ZIP = REPO_ROOT / "logs" / "run_20260407_185002.zip"


def test_reference_run_generates_expected_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "absorbance_debug"

    result = run_debugger(
        REFERENCE_RUN_ZIP,
        output_dir=output_dir,
        ratio_source="raw",
        temperature_source="corr",
        pressure_source="corr",
    )

    assert result["validation_table"]["passed"].all()
    assert (output_dir / "step_01_samples_core.csv").exists()
    assert (output_dir / "step_05_r0_fit_coefficients.csv").exists()
    assert (output_dir / "step_08_old_vs_new_compare.xlsx").exists()
    assert (output_dir / "step_08_overview_summary.csv").exists()
    assert (output_dir / "step_08_by_temperature.csv").exists()
    assert (output_dir / "step_08_by_concentration_range.csv").exists()
    assert (output_dir / "step_08_zero_special.csv").exists()
    assert (output_dir / "step_08_regression_overall.csv").exists()
    assert (output_dir / "step_08_regression_by_temperature.csv").exists()
    assert (output_dir / "step_08_point_reconciliation.csv").exists()
    assert (output_dir / "step_08_auto_conclusions.csv").exists()
    assert (output_dir / "report.md").exists()
    assert (output_dir / "report.html").exists()
    assert (output_dir / "report.xlsx").exists()

    filtered = pd.read_csv(output_dir / "step_02_samples_filtered.csv")
    excluded = pd.read_csv(output_dir / "step_02_excluded_rows.csv")
    assert sorted(filtered["analyzer"].unique().tolist()) == ["GA01", "GA02", "GA03"]
    assert excluded["exclude_reason"].str.contains("warning_only_analyzer").any()

    overview = pd.read_csv(output_dir / "step_08_overview_summary.csv")
    assert set(overview["analyzer_id"]) == {"GA01", "GA02", "GA03"}
    assert {"winner_overall", "winner_zero", "winner_temp_stability", "recommendation"} <= set(overview.columns)

    point_reconciliation = pd.read_csv(output_dir / "step_08_point_reconciliation.csv")
    assert {"old_pred_ppm", "new_pred_ppm", "old_error", "new_error", "winner_for_point"} <= set(point_reconciliation.columns)
    assert point_reconciliation["pressure_source"].isin(["P_std", "P_corr"]).all()
    assert point_reconciliation["temperature_source"].isin(["T_std", "T_corr"]).all()


def test_run_bundle_discovers_zip_and_extracted_directory(tmp_path: Path) -> None:
    zip_bundle = RunBundle(REFERENCE_RUN_ZIP)
    zip_artifacts = discover_run_artifacts(zip_bundle)
    assert zip_artifacts.files["samples"] is not None
    assert zip_artifacts.files["points_readable"] is not None

    extracted_root = tmp_path / "extracted"
    with zipfile.ZipFile(REFERENCE_RUN_ZIP) as archive:
        archive.extractall(extracted_root)

    dir_bundle = RunBundle(extracted_root)
    dir_artifacts = discover_run_artifacts(dir_bundle)
    points = dir_bundle.read_csv(dir_artifacts.files["points_readable"])

    assert dir_artifacts.files["runtime_config"] is not None
    assert len(points) == 48


def test_option_normalizers_accept_gui_and_cli_tokens() -> None:
    assert normalize_ratio_source("raw") == "ratio_co2_raw"
    assert normalize_ratio_source("filt") == "ratio_co2_filt"
    assert normalize_temp_source("T_std") == "temp_std_c"
    assert normalize_temp_source("corr") == "temp_corr_c"
    assert normalize_pressure_source("P_std") == "pressure_std_hpa"
    assert normalize_pressure_source("corr") == "pressure_corr_hpa"
