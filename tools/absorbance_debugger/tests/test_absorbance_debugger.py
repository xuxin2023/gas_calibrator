from __future__ import annotations

import ast
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from tools.absorbance_debugger import gui as gui_module
from tools.absorbance_debugger.analysis.absorbance_models import (
    PIECEWISE_MODEL_SPECS,
    _design_matrix,
    active_model_specs,
    evaluate_absorbance_models,
)
from tools.absorbance_debugger.analysis.pipeline import _identify_invalid_pressure_points
from tools.absorbance_debugger.analysis.zero_residual import (
    build_zero_residual_point_variants,
    fit_zero_residual_models,
)
from tools.absorbance_debugger.app import run_debugger, run_debugger_batch
from tools.absorbance_debugger.io.run_bundle import RunBundle, discover_run_artifacts
from tools.absorbance_debugger.models.config import DebuggerConfig
from tools.absorbance_debugger.options import (
    normalize_absorbance_order_mode,
    normalize_invalid_pressure_mode,
    normalize_model_selection_strategy,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
    parse_path_list,
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
    assert (output_dir / "step_05y_zero_residual_observations.csv").exists()
    assert (output_dir / "step_05y_zero_residual_models.csv").exists()
    assert (output_dir / "step_05y_zero_residual_selection.csv").exists()
    assert (output_dir / "step_05y_zero_residual_plot.png").exists()
    assert (output_dir / "step_06_absorbance_model_candidates.csv").exists()
    assert (output_dir / "step_06_absorbance_model_scores.csv").exists()
    assert (output_dir / "step_06_absorbance_model_selection.csv").exists()
    assert (output_dir / "step_06_absorbance_model_coefficients.csv").exists()
    assert (output_dir / "step_06_absorbance_model_residuals.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_candidates.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_scores.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_selection.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_coefficients.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_plots.png").exists()
    assert (output_dir / "step_06y_weight_sensitivity_compare.csv").exists()
    assert (output_dir / "step_02x_invalid_pressure_points.csv").exists()
    assert (output_dir / "step_02x_invalid_pressure_summary.csv").exists()
    assert (output_dir / "step_02x_invalid_pressure_plots.png").exists()
    assert (output_dir / "step_06x_absorbance_order_compare.csv").exists()
    assert (output_dir / "step_05x_r0_source_consistency.csv").exists()
    assert (output_dir / "step_04x_pressure_branch_compare.csv").exists()
    assert (output_dir / "step_08x_upper_bound_vs_deployable.csv").exists()
    assert (output_dir / "step_08x_root_cause_ranking.csv").exists()
    assert (output_dir / "step_08x_valid_only_overview_summary.csv").exists()
    assert (output_dir / "step_08x_valid_only_by_temperature.csv").exists()
    assert (output_dir / "step_08x_valid_only_zero_special.csv").exists()
    assert (output_dir / "step_08x_valid_only_auto_conclusions.csv").exists()
    assert (output_dir / "step_08x_default_chain_before_after.csv").exists()
    assert (output_dir / "step_08y_ga01_residual_profile.csv").exists()
    assert (output_dir / "step_08y_ga01_residual_profile_plots.png").exists()
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
    assert {"winner_overall", "winner_zero", "winner_temp_stability", "winner_low_range", "winner_main_range", "recommendation"} <= set(overview.columns)

    selection = pd.read_csv(output_dir / "step_06_absorbance_model_selection.csv")
    assert set(selection["analyzer_id"]) == {"GA01", "GA02", "GA03"}
    assert {"best_absorbance_model", "best_model_family", "zero_residual_mode", "selection_reason", "selected_prediction_scope", "selected_source_pair", "default_absorbance_order"} <= set(selection.columns)
    assert set(selection["selected_source_pair"]) <= {"raw/raw", "filt/filt"}
    assert set(selection["default_absorbance_order"]) == {"samplewise_log_first"}

    scores = pd.read_csv(output_dir / "step_06_absorbance_model_scores.csv")
    assert {"model_id", "model_family", "validation_rmse", "overall_rmse", "composite_score", "composite_score_old_weights", "composite_score_new_weights", "model_rank", "selected_source_pair"} <= set(scores.columns)
    assert scores["model_rank"].notna().all()
    assert not scores["selected_source_pair"].isin(["raw/filt", "filt/raw"]).any()

    order_compare = pd.read_csv(output_dir / "step_06x_absorbance_order_compare.csv")
    assert {"order_mode", "samplewise_log_first_is_better", "significant_order_gain"} <= set(order_compare.columns)
    assert set(order_compare["order_mode"]) == {"samplewise_log_first", "mean_first_log"}

    source_compare = pd.read_csv(output_dir / "step_05x_r0_source_consistency.csv")
    assert {"source_pair_label", "mixed_source_invalid_for_production_default"} <= set(source_compare.columns)
    assert {"raw/raw", "filt/filt", "raw/filt", "filt/raw"} <= set(source_compare["source_pair_label"])

    pressure_branch = pd.read_csv(output_dir / "step_04x_pressure_branch_compare.csv")
    assert {"pressure_branch", "branch_rank", "recommended_pressure_branch"} <= set(pressure_branch.columns)
    assert {"no_pressure_norm", "pressure_std", "pressure_corr"} <= set(pressure_branch["pressure_branch"])

    upper_vs_deployable = pd.read_csv(output_dir / "step_08x_upper_bound_vs_deployable.csv")
    assert {"chain_context", "best_model_upper_bound", "best_model_deployable", "best_model_consistent"} <= set(upper_vs_deployable.columns)
    assert {"physics_upper_bound", "deployable_chain"} <= set(upper_vs_deployable["chain_context"])

    root_causes = pd.read_csv(output_dir / "step_08x_root_cause_ranking.csv")
    assert {"rank", "issue_name", "severity", "evidence", "recommended_action"} <= set(root_causes.columns)
    assert "weak_absorbance_ppm_model" in set(root_causes["issue_name"])

    point_reconciliation = pd.read_csv(output_dir / "step_08_point_reconciliation.csv")
    assert {"old_pred_ppm", "new_pred_ppm", "old_error", "new_error", "winner_for_point", "selected_source_pair"} <= set(point_reconciliation.columns)
    assert point_reconciliation["pressure_source"].isin(["P_std", "P_corr"]).all()
    assert point_reconciliation["temperature_source"].isin(["T_std", "T_corr"]).all()
    assert point_reconciliation["best_absorbance_model"].notna().any()
    assert point_reconciliation["selected_prediction_scope"].isin(["validation_oof", "overall_fit"]).all()
    assert set(point_reconciliation["absorbance_order_mode_selected"]) == {"samplewise_log_first"}
    assert set(point_reconciliation["selected_source_pair"].dropna().unique().tolist()) <= {"raw/raw", "filt/filt"}

    selected_models = selection.set_index("analyzer_id")["best_absorbance_model"].to_dict()
    point_models = (
        point_reconciliation.dropna(subset=["best_absorbance_model"])
        .groupby("analyzer_id")["best_absorbance_model"]
        .agg(lambda values: values.mode().iloc[0])
        .to_dict()
    )
    assert point_models == selected_models


def test_zero_residual_fit_and_compare_can_run(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    points = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "analyzer_slot": 1,
                "point_title": f"p{idx}",
                "point_row": idx,
                "point_tag": f"p{idx}",
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "ratio_source": "ratio_co2_raw",
                "temp_source": "temp_corr_c",
                "pressure_source": "pressure_corr_hpa",
                "r0_model": "quadratic",
                "branch_id": config.default_branch_id(),
                "is_default_branch": True,
                "sample_count": 4,
                "ratio_in_mean": 1.0,
                "temp_use_mean_c": temp_c,
                "pressure_use_mean_hpa": 1013.25,
                "R0_T_mean": 1.0,
                "A_mean": absorbance,
                "A_std": 0.01,
                "A_min": absorbance - 0.01,
                "A_max": absorbance + 0.01,
                "A_alt_mean": absorbance / config.p_ref_hpa,
                "A_from_mean": absorbance,
            }
            for idx, (temp_c, target_ppm, absorbance) in enumerate(
                [
                    (-10.0, 0.0, 2.0),
                    (0.0, 0.0, 1.0),
                    (20.0, 0.0, 0.0),
                    (30.0, 0.0, -0.5),
                    (0.0, 100.0, 12.0),
                    (20.0, 200.0, 20.0),
                    (30.0, 500.0, 48.0),
                ],
                start=1,
            )
        ]
    )
    observations, models, selection, lookup = fit_zero_residual_models(points, config)
    variants = build_zero_residual_point_variants(points, lookup, config)

    assert not observations.empty
    assert not models.empty
    assert not selection.empty
    assert {"none", "linear", "quadratic"} <= set(variants["zero_residual_mode"])
    none_zero = variants[(variants["zero_residual_mode"] == "none") & (variants["target_co2_ppm"] == 0)]["A_mean"].to_numpy()
    quad_zero = variants[(variants["zero_residual_mode"] == "quadratic") & (variants["target_co2_ppm"] == 0)]["A_mean"].to_numpy()
    assert np.sqrt(np.mean(np.square(quad_zero))) < np.sqrt(np.mean(np.square(none_zero)))


def test_piecewise_model_continuity_and_selection_can_run(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    points = []
    for idx, (temp_c, target_ppm, absorbance) in enumerate(
        [
            (0.0, 0.0, 0.0),
            (0.0, 100.0, 8.0),
            (0.0, 200.0, 16.0),
            (0.0, 400.0, 26.0),
            (0.0, 800.0, 42.0),
            (20.0, 0.0, 0.3),
            (20.0, 100.0, 8.4),
            (20.0, 200.0, 16.3),
            (20.0, 400.0, 26.2),
            (20.0, 800.0, 41.7),
        ],
        start=1,
    ):
        points.append(
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "temp_use_mean_c": temp_c,
                "A_mean": absorbance,
                "A_std": 0.01,
                "zero_residual_mode": "none",
                "zero_residual_model_label": "No zero residual correction",
                "with_zero_residual_correction": False,
            }
        )
    frame = pd.DataFrame(points)
    results = evaluate_absorbance_models(frame, config)
    assert "piecewise_range" in set(results["scores"]["model_family"])
    coeffs = results["coefficients"]
    piecewise_row = results["scores"][results["scores"]["model_id"] == "piecewise_linear"].iloc[0]
    coeff_vector = coeffs[coeffs["model_id"] == "piecewise_linear"].sort_values("term_order")["coefficient"].to_numpy(dtype=float)
    breakpoint_absorbance = float(piecewise_row["piecewise_boundary_absorbance"])
    spec = next(spec for spec in active_model_specs(config) if spec.model_id == "piecewise_linear")
    left = pd.DataFrame({"absorbance_input": [breakpoint_absorbance], "temp_model_c": [20.0]})
    right = pd.DataFrame({"absorbance_input": [breakpoint_absorbance + 1.0e-9], "temp_model_c": [20.0]})
    pred_left = float((_design_matrix(left, spec, breakpoint_absorbance) @ coeff_vector)[0])
    pred_right = float((_design_matrix(right, spec, breakpoint_absorbance) @ coeff_vector)[0])
    assert abs(pred_left - pred_right) < 1.0e-4


def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None:
    run_a = tmp_path / "run_a.zip"
    run_b = tmp_path / "run_b.zip"
    run_a.write_text("a", encoding="utf-8")
    run_b.write_text("b", encoding="utf-8")

    def fake_run_debugger(input_path, **kwargs):
        stem = Path(input_path).stem
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        overview = pd.DataFrame(
            [
                {"analyzer_id": "GA01", "old_chain_rmse": 5.0, "new_chain_rmse": 7.0, "winner_overall": "old_chain", "winner_zero": "old_chain", "winner_temp_stability": "old_chain", "winner_low_range": "old_chain", "winner_main_range": "old_chain"},
                {"analyzer_id": "GA02", "old_chain_rmse": 6.0, "new_chain_rmse": 4.0 if stem == "run_a" else 5.0, "winner_overall": "new_chain", "winner_zero": "new_chain", "winner_temp_stability": "new_chain", "winner_low_range": "new_chain", "winner_main_range": "new_chain"},
                {"analyzer_id": "GA03", "old_chain_rmse": 5.5, "new_chain_rmse": 4.5, "winner_overall": "new_chain", "winner_zero": "new_chain", "winner_temp_stability": "new_chain", "winner_low_range": "new_chain", "winner_main_range": "new_chain"},
            ]
        )
        selection = pd.DataFrame(
            [
                {"analyzer_id": "GA01", "best_absorbance_model": "model_a_linear", "best_model_family": "single_range", "zero_residual_mode": "none", "selected_source_pair": "raw/raw"},
                {"analyzer_id": "GA02", "best_absorbance_model": "piecewise_linear", "best_model_family": "piecewise_range", "zero_residual_mode": "linear", "selected_source_pair": "filt/filt"},
                {"analyzer_id": "GA03", "best_absorbance_model": "piecewise_quadratic", "best_model_family": "piecewise_range", "zero_residual_mode": "quadratic", "selected_source_pair": "raw/raw"},
            ]
        )
        return {
            "output_dir": output_dir,
            "validation_table": pd.DataFrame(),
            "comparison_outputs": {"overview_summary": overview},
            "model_results": {"selection": selection},
            "ga01_special_note": "GA01 still looks like a high-temp zero-anchor issue.",
        }

    monkeypatch.setattr("tools.absorbance_debugger.app.run_debugger", fake_run_debugger)
    result = run_debugger_batch((run_a, run_b), output_dir=tmp_path / "batch")

    assert (Path(result["output_dir"]) / "step_09_cross_run_summary.csv").exists()
    assert (Path(result["output_dir"]) / "step_09_cross_run_plots.png").exists()
    assert "GA02" in result["reproducibility_note"]
    assert set(result["cross_run_summary"]["run_name"]) == {"run_a", "run_b"}


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
    assert normalize_absorbance_order_mode("samplewise") == "samplewise_log_first"
    assert normalize_absorbance_order_mode("compare_both") == "compare_both"
    assert normalize_invalid_pressure_mode("hard") == "hard_exclude"
    assert normalize_invalid_pressure_mode("diagnostic") == "diagnostic_only"
    assert normalize_model_selection_strategy("auto") == "auto_grouped"
    assert normalize_model_selection_strategy("grouped_loo") == "grouped_loo"


def test_invalid_pressure_filter_hard_excludes_500_hpa_bin(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    filtered = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": "p500",
                "point_row": 1,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 500.0,
                "pressure_std_hpa": 503.0,
                "pressure_corr_hpa": 501.0,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "p500",
                "point_row": 1,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 500.0,
                "pressure_std_hpa": 504.0,
                "pressure_corr_hpa": 502.0,
                "sample_index": 2,
            },
            {
                "analyzer": "GA01",
                "point_title": "p1013",
                "point_row": 2,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 1013.25,
                "pressure_std_hpa": 1012.0,
                "pressure_corr_hpa": 1013.0,
                "sample_index": 1,
            },
        ]
    )
    invalid_points, invalid_summary, filtered_valid, excluded_invalid = _identify_invalid_pressure_points(filtered, config)

    assert len(invalid_points) == 1
    assert bool(invalid_points.iloc[0]["excluded_from_main_analysis"]) is True
    assert invalid_points.iloc[0]["invalid_reason"] == "legacy_invalid_pressure_target_500hpa"
    assert len(filtered_valid) == 1
    assert len(excluded_invalid) == 2
    assert int(invalid_summary.iloc[0]["invalid_point_count"]) == 1


def test_gui_passes_selection_parameters(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run_debugger(input_path, **kwargs):
        captured["input_path"] = str(input_path)
        captured.update(kwargs)
        return {"output_dir": str(tmp_path)}

    class ImmediateThread:
        def __init__(self, target, daemon) -> None:
            self._target = target

        def start(self) -> None:
            self._target()

    root = gui_module.Tk()
    root.withdraw()
    gui = gui_module.AbsorbanceDebuggerGui(root)
    gui.root.after = lambda _delay, callback: callback()
    gui.input_path.set(str(REFERENCE_RUN_ZIP))
    gui.output_dir.set(str(tmp_path))
    gui.p_ref.set("1009.5")
    gui.ratio_source.set("filt")
    gui.temperature_source.set("T_std")
    gui.pressure_source.set("P_std")
    gui.absorbance_order_mode.set("mean_first_log")
    gui.model_selection_strategy.set("grouped_loo")
    gui.enable_zero_residual_correction.set("1")
    gui.zero_residual_models.set("quadratic")
    gui.enable_piecewise_model.set("1")
    gui.piecewise_boundary_ppm.set("180")
    gui.invalid_pressure_targets_hpa.set("500,530")
    gui.invalid_pressure_tolerance_hpa.set("25")
    gui.enable_composite_score.set("0")
    gui.run_source_consistency_compare.set("0")
    gui.run_pressure_branch_compare.set("1")
    gui.run_upper_bound_compare.set("0")
    gui.hard_invalid_pressure_exclude.set("1")
    gui.use_valid_only_main_conclusion.set("1")
    gui.auto_open_report.set("0")

    monkeypatch.setattr(gui_module, "run_debugger", fake_run_debugger)
    monkeypatch.setattr(gui_module.threading, "Thread", lambda target, daemon=True: ImmediateThread(target, daemon))

    gui._start_analysis()
    root.destroy()

    assert captured["input_path"] == str(REFERENCE_RUN_ZIP)
    assert captured["ratio_source"] == "ratio_co2_filt"
    assert captured["temperature_source"] == "temp_std_c"
    assert captured["pressure_source"] == "pressure_std_hpa"
    assert captured["absorbance_order_mode"] == "mean_first_log"
    assert captured["model_selection_strategy"] == "grouped_loo"
    assert captured["enable_composite_score"] is False
    assert captured["enable_zero_residual_correction"] is True
    assert captured["zero_residual_models"] == "quadratic"
    assert captured["enable_piecewise_model"] is True
    assert captured["piecewise_boundary_ppm"] == 180.0
    assert captured["run_r0_source_consistency_compare"] is False
    assert captured["run_pressure_branch_compare"] is True
    assert captured["run_upper_bound_compare"] is False
    assert captured["invalid_pressure_targets_hpa"] == "500,530"
    assert captured["invalid_pressure_tolerance_hpa"] == 25.0
    assert captured["invalid_pressure_mode"] == "hard_exclude"
    assert captured["use_valid_only_main_conclusion"] is True
    assert captured["p_ref_hpa"] == 1009.5


def test_tool_has_no_runtime_import_to_v1() -> None:
    tool_root = REPO_ROOT / "tools" / "absorbance_debugger"
    offenders: list[str] = []
    for path in tool_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                names = [module]
            else:
                continue
            if any(name == "run_app" or name.startswith("src.gas_calibrator") for name in names):
                offenders.append(str(path))
                break
    assert offenders == []


def test_parse_path_list_supports_batch_gui_entry() -> None:
    assert parse_path_list("a.zip;b.zip\nc") == ("a.zip", "b.zip", "c")
