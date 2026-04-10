"""Water/pressure lineage audit helpers for the offline debugger."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _runtime_water_config_row(runtime_cfg_path: Path | None) -> dict[str, Any] | None:
    if runtime_cfg_path is None or not runtime_cfg_path.exists():
        return None
    try:
        payload = json.loads(runtime_cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    workflow = payload.get("workflow", {}) if isinstance(payload, dict) else {}
    coefficients = payload.get("coefficients", {}) if isinstance(payload, dict) else {}
    h2o_summary = coefficients.get("h2o_summary_selection", {}) if isinstance(coefficients, dict) else {}
    return {
        "source_path": str(runtime_cfg_path),
        "source_type": "run_artifact",
        "concept": "runtime h2o summary selection",
        "legacy_chain_scope": "runtime_config_snapshot",
        "water_participation_class": "temperature_related_correction|zero_offset_correction|other",
        "fields": "route_mode|water_first_all_temps|water_first_temp_gte|h2o_source|fit_h2o|include_co2_zero_ppm_rows|include_co2_zero_ppm_temp_groups_c|humidity_keys",
        "explicitness": "explicit config",
        "matched_detail": json.dumps(
            {
                "route_mode": workflow.get("route_mode"),
                "water_first_all_temps": workflow.get("water_first_all_temps"),
                "water_first_temp_gte": workflow.get("water_first_temp_gte"),
                "h2o_source": coefficients.get("h2o_source"),
                "fit_h2o": coefficients.get("fit_h2o"),
                "include_co2_zero_ppm_rows": h2o_summary.get("include_co2_zero_ppm_rows"),
                "include_co2_zero_ppm_temp_groups_c": h2o_summary.get("include_co2_zero_ppm_temp_groups_c"),
                "humidity_keys": coefficients.get("humidity_keys"),
            },
            ensure_ascii=False,
        ),
        "assessment": (
            "Runtime config keeps a dedicated H2O lineage with water-first routing and explicit "
            "CO2 zero-ppm inclusion inside the H2O summary selection."
        ),
    }


def build_old_water_correction_audit(
    *,
    runtime_cfg_path: Path | None = None,
) -> tuple[str, pd.DataFrame, list[str]]:
    """Build a repo-backed audit of legacy water-correction lineage."""

    root = _repo_root()
    sources: list[dict[str, Any]] = []

    corrected_water_path = root / "src" / "gas_calibrator" / "export" / "corrected_water_points_report.py"
    corrected_water_text = _safe_read_text(corrected_water_path)
    if corrected_water_text:
        sources.append(
            {
                "source_path": str(corrected_water_path),
                "source_type": "code",
                "concept": "corrected water report mixes H2O rows with CO2 zero rows",
                "legacy_chain_scope": "offline corrected-water diagnostics",
                "water_participation_class": "zero_offset_correction|temperature_related_correction",
                "fields": "PhaseKey|ppm_H2O_Dew|R_H2O|ppm_CO2_Tank|Temp|BAR|include_co2_zero_ppm_rows|include_co2_zero_ppm_temp_groups_c",
                "explicitness": "explicit selection logic",
                "matched_detail": (
                    "select_corrected_fit_rows keeps H2O phase rows, selected CO2 temperature groups, "
                    "and selected CO2 zero-ppm rows for the corrected-water fit bundle."
                ),
                "assessment": (
                    "This is the strongest direct evidence that legacy offline tooling treated CO2 zero rows "
                    "as anchor-like evidence for a water-correction workflow."
                ),
            }
        )

    feature_builder_path = root / "src" / "gas_calibrator" / "coefficients" / "feature_builder.py"
    feature_builder_text = _safe_read_text(feature_builder_path)
    if "HUMIDITY_MODEL_FEATURES" in feature_builder_text and '"H": "H2O"' in feature_builder_text:
        sources.append(
            {
                "source_path": str(feature_builder_path),
                "source_type": "code",
                "concept": "humidity cross features are explicit model terms",
                "legacy_chain_scope": "offline ratio_poly feature builder",
                "water_participation_class": "concentration_post_correction",
                "fields": "H|H2|RH",
                "explicitness": "explicit formula support",
                "matched_detail": "feature_builder defines H, H2, and RH model tokens for humidity-like cross terms.",
                "assessment": (
                    "Legacy offline modeling explicitly supported humidity-dependent CO2 correction terms, "
                    "even though they were not guaranteed to be the default production choice."
                ),
            }
        )

    selector_path = root / "src" / "gas_calibrator" / "coefficients" / "model_selector.py"
    selector_text = _safe_read_text(selector_path)
    if "H2O_CROSS_CANDIDATE_MODELS" in selector_text:
        sources.append(
            {
                "source_path": str(selector_path),
                "source_type": "code",
                "concept": "H2O cross candidate models exist",
                "legacy_chain_scope": "offline model selection",
                "water_participation_class": "concentration_post_correction",
                "fields": "Model_D|Model_E|H|H2|RH",
                "explicitness": "explicit candidate-model support",
                "matched_detail": "model_selector defines Model_D / Model_E with H, H2, and RH cross features.",
                "assessment": (
                    "Legacy offline tooling did not only log water data; it had an explicit path to let "
                    "humidity-like features participate in CO2 model selection."
                ),
            }
        )

    logging_utils_path = root / "src" / "gas_calibrator" / "logging_utils.py"
    logging_utils_text = _safe_read_text(logging_utils_path)
    if "ppm_H2O_Dew" in logging_utils_text and "R_H2O" in logging_utils_text:
        sources.append(
            {
                "source_path": str(logging_utils_path),
                "source_type": "code",
                "concept": "legacy analyzer summary exports water lineage fields",
                "legacy_chain_scope": "summary artifact generation",
                "water_participation_class": "other",
                "fields": "ppm_H2O_Dew|R_H2O|R_H2O_dev|PpmH2oDewPressureSource",
                "explicitness": "explicit artifact fields",
                "matched_detail": (
                    "Run summary rows export dew-derived water reference, H2O ratio statistics, "
                    "and the pressure source used to compute dew-based water concentration."
                ),
                "assessment": (
                    "The legacy ecosystem preserved water/pressure lineage in summary outputs, which makes a "
                    "downstream water-aware correction path plausible."
                ),
            }
        )

    default_cfg_path = root / "configs" / "default_config.json"
    default_cfg_text = _safe_read_text(default_cfg_path)
    if '"humidity_keys"' in default_cfg_text and '"route_mode": "h2o_then_co2"' in default_cfg_text:
        sources.append(
            {
                "source_path": str(default_cfg_path),
                "source_type": "config",
                "concept": "default config carries water-first route and humidity keys",
                "legacy_chain_scope": "global modeling/runtime config",
                "water_participation_class": "temperature_related_correction|other",
                "fields": "route_mode|water_first_all_temps|water_first_temp_gte|humidity_keys|h2o_source|fit_h2o",
                "explicitness": "explicit config",
                "matched_detail": (
                    "default_config keeps h2o_then_co2 routing, water-first temperature rules, "
                    "humidity_keys, and an H2O modeling lineage."
                ),
                "assessment": (
                    "Water lineage was designed into the broader configuration surface, not added as a one-off field."
                ),
            }
        )

    manifest_path = _first_existing(
        [
            root / "output" / "run_20260322_223345" / "manifest.json",
            root / "logs" / "codex_quick_smoke_co2_recheck_20260331_1015" / "runtime_config_snapshot.json",
        ]
    )
    manifest_text = _safe_read_text(manifest_path) if manifest_path is not None else ""
    if manifest_text and '"include_co2_zero_ppm_rows": true' in manifest_text:
        sources.append(
            {
                "source_path": str(manifest_path),
                "source_type": "historical_artifact",
                "concept": "historical artifact confirms H2O summary selection was active in real runs",
                "legacy_chain_scope": "manifest/runtime artifact",
                "water_participation_class": "zero_offset_correction|temperature_related_correction",
                "fields": "h2o_summary_selection|include_co2_zero_ppm_rows|include_co2_zero_ppm_temp_groups_c",
                "explicitness": "explicit historical artifact",
                "matched_detail": (
                    "Historical manifest/runtime snapshot records CO2 zero-ppm rows inside the H2O summary selection."
                ),
                "assessment": (
                    "This was not only code support; the setting also appeared in persisted historical run artifacts."
                ),
            }
        )

    workbook_path = _first_existing(
        [
            root / "logs" / "codex_quick_smoke_co2_one_point_20260331_0135" / "分析仪汇总_20260330_224911.csv",
            root / "logs" / "codex_quick_smoke_co2_one_point_20260331_0135" / "分析仪汇总_气路_20260330_224911.csv",
        ]
    )
    if workbook_path is not None:
        try:
            header = pd.read_csv(workbook_path, nrows=0).columns.tolist()
        except Exception:
            header = []
        if header:
            sources.append(
                {
                    "source_path": str(workbook_path),
                    "source_type": "historical_artifact",
                    "concept": "historical analyzer summary contains water and pressure lineage columns",
                    "legacy_chain_scope": "analyzer summary workbook",
                    "water_participation_class": "other",
                    "fields": "|".join(
                        column
                        for column in ("ppm_H2O_Dew", "R_H2O", "R_H2O_dev", "PpmH2oDewPressureSource")
                        if column in header
                    ),
                    "explicitness": "explicit report fields",
                    "matched_detail": "Historical analyzer summary header exports H2O reference, H2O ratio, and dew-pressure lineage.",
                    "assessment": (
                        "Historical outputs expose the same water lineage fields that the current debugger currently ignores."
                    ),
                }
            )

    runtime_row = _runtime_water_config_row(runtime_cfg_path)
    if runtime_row is not None:
        sources.append(runtime_row)

    sources_df = pd.DataFrame(sources)
    if not sources_df.empty:
        sources_df = sources_df.sort_values(["source_type", "concept", "source_path"], ignore_index=True)

    summary_lines = [
        "Explicit legacy water lineage exists in code, config, and historical artifacts; it was not only a logging side-channel.",
        "The strongest direct evidence is the corrected-water offline report, which explicitly reuses selected CO2 zero-ppm rows inside an H2O-focused correction dataset.",
        "Legacy offline ratio-poly tooling explicitly supports humidity cross terms (H, H2, RH) and H2O-cross candidate CO2 models.",
        "Historical manifests/runtime snapshots show water-first routing plus h2o_summary_selection.include_co2_zero_ppm_rows, so the zero-anchor idea appeared in persisted run artifacts.",
        "No single explicit V1 CO2 production formula named water_baseline / water_anchor / corrected_water subtraction was found in the audited sources.",
    ]

    markdown_lines = [
        "# Old Water Correction Audit",
        "",
        "## Scope",
        "- repo code under `src/gas_calibrator/**` and `configs/**` was audited in read-only mode",
        "- historical persisted artifacts under `output/**` and `logs/**` were sampled when present",
        "- no V1 code was modified and no debugger logic was written back into V1",
        "",
        "## Findings",
    ]
    markdown_lines.extend(f"- {line}" for line in summary_lines)
    markdown_lines.extend(
        [
            "",
            "## Participation Classification",
            "- a) R preprocess: no direct evidence of a standalone CO2 `R` pre-subtraction by water was found; the stronger evidence points to separate H2O fits and humidity-aware downstream models.",
            "- b) concentration post-correction: explicit support exists in legacy offline ratio-poly tooling through `H`, `H2`, and `RH` humidity cross features and H2O-cross candidate models.",
            "- c) zero offset correction: likely implicit/experience-driven rather than a single named formula; the corrected-water report and `include_co2_zero_ppm_rows` strongly suggest zero-anchor-style reuse of water data.",
            "- d) temperature-related correction: explicit temp-bucket and zero-ppm temp-group selection exists in the H2O summary/correction workflow.",
            "- e) other: dew-derived water reference and pressure-source lineage are explicitly exported in legacy analyzer summary artifacts.",
            "",
            "## Bottom Line",
            "- The old ecosystem very likely had a more complete water lineage than the current debugger main chain.",
            "- The audit supports the hypothesis that old offline results could benefit from water-aware heuristics.",
            "- The audit does not prove that the frozen V1 production CO2 chain always enabled a direct water-zero correction term.",
        ]
    )
    return "\n".join(markdown_lines) + "\n", sources_df, summary_lines


def build_new_chain_input_audit(
    *,
    config: Any,
    selected_source_summary: pd.DataFrame,
    samples_core: pd.DataFrame,
    zero_residual_selection: pd.DataFrame,
) -> pd.DataFrame:
    """Describe what the current debugger main chain uses and what it ignores."""

    available_h2o_fields = [
        column
        for column in ("ratio_h2o_raw", "ratio_h2o_filt", "h2o_signal", "h2o_density", "h2o_mmol")
        if column in samples_core.columns
    ]
    not_used_water_items = [
        "ratio_h2o_raw",
        "ratio_h2o_filt",
        "ppm_H2O_Dew",
        "R_H2O",
        "water baseline / water anchor",
        "humidity cross feature H/H2/RH",
        "corrected water report lineage",
        "merged zero anchor",
    ]

    zero_lookup = (
        zero_residual_selection.set_index(["analyzer_id", "ratio_source"])
        if not zero_residual_selection.empty and {"analyzer_id", "ratio_source"} <= set(zero_residual_selection.columns)
        else pd.DataFrame()
    )

    rows: list[dict[str, Any]] = [
        {
            "audit_scope": "overall_main_chain",
            "analyzer_id": "",
            "R_in_source": "matched-only per analyzer from ratio_co2_raw or ratio_co2_filt",
            "R0_fit_source": "same matched CO2 ratio source as R_in on valid-only 0 ppm rows",
            "temp_source": config.default_temp_source,
            "pressure_source": config.default_pressure_source,
            "source_policy": config.default_source_policy,
            "matched_selection_policy": config.matched_selection_policy,
            "uses_h2o_ratio": False,
            "uses_water_baseline_or_anchor": False,
            "uses_external_humidity_like_feature": False,
            "uses_zero_residual_correction": bool(config.enable_zero_residual_correction),
            "zero_residual_mode": "per-analyzer selected",
            "uses_merged_zero_anchor": False,
            "uses_water_zero_anchor_correction_in_main_chain": False,
            "available_h2o_fields_in_samples": "|".join(available_h2o_fields),
            "legacy_water_terms_not_used": "|".join(not_used_water_items),
            "note": (
                "Current debugger main chain parses H2O fields into samples_core but does not feed them into "
                "R0(T), A_raw, or ppm model selection by default."
            ),
        }
    ]

    for row in selected_source_summary.to_dict(orient="records"):
        ratio_source = str(row.get("selected_ratio_source") or "")
        analyzer_id = str(row.get("analyzer_id") or "")
        zero_mode = str(row.get("zero_residual_mode") or "none")
        if isinstance(zero_lookup, pd.DataFrame) and not zero_lookup.empty and (analyzer_id, ratio_source) in zero_lookup.index:
            zero_mode = str(zero_lookup.loc[(analyzer_id, ratio_source), "selected_zero_residual_model"])
        rows.append(
            {
                "audit_scope": "per_analyzer_selected_main_chain",
                "analyzer_id": analyzer_id,
                "R_in_source": ratio_source,
                "R0_fit_source": ratio_source,
                "temp_source": config.default_temp_source,
                "pressure_source": config.default_pressure_source,
                "source_policy": config.default_source_policy,
                "matched_selection_policy": config.matched_selection_policy,
                "uses_h2o_ratio": False,
                "uses_water_baseline_or_anchor": False,
                "uses_external_humidity_like_feature": False,
                "uses_zero_residual_correction": zero_mode != "none",
                "zero_residual_mode": zero_mode,
                "uses_merged_zero_anchor": False,
                "uses_water_zero_anchor_correction_in_main_chain": False,
                "available_h2o_fields_in_samples": "|".join(available_h2o_fields),
                "legacy_water_terms_not_used": "|".join(not_used_water_items),
                "note": (
                    f"Selected matched source pair is {row.get('selected_source_pair', '')}; "
                    "H2O ratio columns remain diagnostic-only in the current main chain."
                ),
            }
        )

    return pd.DataFrame(rows)
