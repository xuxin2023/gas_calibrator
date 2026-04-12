"""Apply the step-25 runtime temperature normalization fix to a production bundle.

This tool keeps the scope intentionally narrow:
- reuse the production bundle's existing ratio-poly base model
- reuse debugger-handoff temperature coefficients and mapping metadata
- patch only the native CO2 ppm surface written into the bundle
- emit minimal observability for before/after comparison and validator replay
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_VERSION_TAG = "runtime-temp-fix-v1"


@dataclass(frozen=True)
class AnalyzerTempFixSpec:
    analyzer_id: str
    slot: int
    temp_column: str
    temp_source: str
    loaded_temp_intercept: float
    loaded_temp_slope: float
    denominator_expected: str
    denominator_observed: str
    expected_formula: str
    recommended_formula: str
    recommended_stage: str
    coefficient_mapping_table: str
    coefficient_sign_match_flag: bool
    coefficient_order_match_flag: bool
    scale_ratio_loaded_to_native: float
    before_normalization_proxy: str
    after_normalization_proxy: str


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_analyzer_specs(
    *,
    step17_summary_path: Path,
    step19_summary_path: Path,
    step25_spec_path: Path,
) -> dict[str, AnalyzerTempFixSpec]:
    step17_rows = _load_csv_rows(step17_summary_path)
    step19_rows = _load_csv_rows(step19_summary_path)
    step25_rows = _load_csv_rows(step25_spec_path)

    step17_by_analyzer = {
        str(row.get("analyzer_id") or "").strip(): row
        for row in step17_rows
        if str(row.get("summary_scope") or "").strip() == "analyzer_overall"
    }
    step19_by_analyzer = {
        str(row.get("analyzer_id") or "").strip(): row
        for row in step19_rows
        if str(row.get("summary_scope") or "").strip() == "analyzer_overall"
    }
    step25_by_analyzer = {
        str(row.get("analyzer_id") or "").strip(): row
        for row in step25_rows
        if str(row.get("summary_scope") or "").strip() == "analyzer_overall"
    }

    specs: dict[str, AnalyzerTempFixSpec] = {}
    for analyzer_id, step17_row in step17_by_analyzer.items():
        if analyzer_id not in step19_by_analyzer or analyzer_id not in step25_by_analyzer:
            continue
        slot_text = analyzer_id.upper().replace("GA", "")
        try:
            slot = int(slot_text)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid analyzer id in debugger outputs: {analyzer_id}") from exc

        offline_coeffs = _parse_json_dict(step17_row.get("offline_temp_coefficients"))
        mapping_table = _parse_json_dict(step25_by_analyzer[analyzer_id].get("coefficient_mapping_table"))
        runtime_loaded_proxy = _parse_json_dict(mapping_table.get("runtime_loaded_proxy"))

        temp_column = str(offline_coeffs.get("column") or offline_coeffs.get("source") or "").strip()
        if not temp_column:
            raise ValueError(f"Missing temp column mapping for {analyzer_id} in {step17_summary_path}")

        before_proxy = str(step25_by_analyzer[analyzer_id].get("observed_runtime_temp_normalization_proxy") or "").strip()
        specs[analyzer_id] = AnalyzerTempFixSpec(
            analyzer_id=analyzer_id,
            slot=slot,
            temp_column=temp_column,
            temp_source=str(offline_coeffs.get("source") or temp_column),
            loaded_temp_intercept=float(runtime_loaded_proxy.get("temp_intercept")),
            loaded_temp_slope=float(runtime_loaded_proxy.get("temp_slope")),
            denominator_expected=str(step25_by_analyzer[analyzer_id].get("denominator_or_range_expected") or "").strip(),
            denominator_observed=str(step25_by_analyzer[analyzer_id].get("denominator_or_range_observed") or "").strip(),
            expected_formula=str(step25_by_analyzer[analyzer_id].get("expected_temp_normalization_formula") or "").strip(),
            recommended_formula=str(step25_by_analyzer[analyzer_id].get("recommended_fix_formula") or "").strip(),
            recommended_stage=str(step25_by_analyzer[analyzer_id].get("recommended_fix_stage") or "").strip(),
            coefficient_mapping_table=json.dumps(mapping_table, ensure_ascii=False),
            coefficient_sign_match_flag=_safe_bool(step17_row.get("coefficient_sign_match_flag")),
            coefficient_order_match_flag=_safe_bool(step17_row.get("coefficient_order_match_flag")),
            scale_ratio_loaded_to_native=float(step19_by_analyzer[analyzer_id].get("scale_ratio_loaded_to_native")),
            before_normalization_proxy=before_proxy,
            after_normalization_proxy="temp_norm_fixed := temp_norm_expected (expected denominator/range restored)",
        )
    if not specs:
        raise ValueError("No analyzer temperature fix specs were loaded from debugger outputs")
    return specs


def _discover_bundle_root(bundle_path: Path, temp_dir: Path) -> tuple[Path, Path]:
    if bundle_path.is_dir():
        return bundle_path, bundle_path
    if bundle_path.suffix.lower() != ".zip":
        raise ValueError(f"Unsupported bundle path: {bundle_path}")
    with zipfile.ZipFile(bundle_path, "r") as archive:
        archive.extractall(temp_dir)
    children = [child for child in temp_dir.iterdir() if child.is_dir()]
    if len(children) == 1:
        return children[0], temp_dir
    return temp_dir, temp_dir


def _first_existing(path_candidates: list[str], frame: pd.DataFrame) -> str:
    for candidate in path_candidates:
        if candidate in frame.columns:
            return candidate
    return ""


def _evaluate_ratio_poly(coefficients: dict[str, Any], ratio: float, temp_c: float, pressure_hpa: float) -> float:
    temp_k = float(temp_c) + 273.15
    return float(
        float(coefficients.get("a0", 0.0))
        + float(coefficients.get("a1", 0.0)) * float(ratio)
        + float(coefficients.get("a2", 0.0)) * float(ratio) ** 2
        + float(coefficients.get("a3", 0.0)) * float(ratio) ** 3
        + float(coefficients.get("a4", 0.0)) * temp_k
        + float(coefficients.get("a5", 0.0)) * temp_k**2
        + float(coefficients.get("a6", 0.0)) * float(ratio) * temp_k
        + float(coefficients.get("a7", 0.0)) * float(pressure_hpa)
        + float(coefficients.get("a8", 0.0)) * float(ratio) * temp_k * float(pressure_hpa)
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _point_temp_input(
    row: pd.Series,
    *,
    spec: AnalyzerTempFixSpec,
    cavity_column: str,
    shell_column: str,
    point_temp_column: str,
    point_set_temp_column: str,
) -> tuple[float | None, str]:
    cavity_value = _safe_float(row.get(cavity_column)) if cavity_column else None
    shell_value = _safe_float(row.get(shell_column)) if shell_column else None
    point_temp_value = _safe_float(row.get(point_temp_column)) if point_temp_column else None
    point_set_value = _safe_float(row.get(point_set_temp_column)) if point_set_temp_column else None

    if spec.temp_column == "candidate_temp_shell_c":
        return shell_value, "candidate_temp_shell_c"
    if spec.temp_column == "candidate_temp_cavity_or_chamber_c":
        return cavity_value, "candidate_temp_cavity_or_chamber_c"
    if spec.temp_column == "candidate_temp_fitted_ttrue_proxy_c":
        return point_temp_value, "candidate_temp_fitted_ttrue_proxy_c"
    if spec.temp_column == "candidate_temp_input_c":
        if cavity_value is not None:
            return cavity_value, "candidate_temp_input_c(cavity)"
        if shell_value is not None:
            return shell_value, "candidate_temp_input_c(shell)"
        if point_temp_value is not None:
            return point_temp_value, "candidate_temp_input_c(point_temp)"
        return point_set_value, "candidate_temp_input_c(point_set)"
    if spec.temp_column == "candidate_temp_feature_c":
        if cavity_value is not None:
            return cavity_value, "candidate_temp_feature_c(cavity)"
        if shell_value is not None:
            return shell_value, "candidate_temp_feature_c(shell)"
        return point_temp_value, "candidate_temp_feature_c(point_temp)"
    return point_temp_value, spec.temp_column or "point_temp"


def _safe_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _find_run_artifact(run_dir: Path, pattern: str) -> Path:
    matches = sorted(run_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"Missing artifact matching {pattern} under {run_dir}")
    return matches[0]


def _load_point_fix_frame(run_dir: Path, specs: dict[str, AnalyzerTempFixSpec]) -> pd.DataFrame:
    points_path = _find_run_artifact(run_dir, "points_*.csv")
    points = pd.read_csv(points_path, encoding="utf-8-sig")

    point_row_column = "校准点行号"
    point_title_column = "点位标题"
    stage_column = "流程阶段"
    target_column = "目标二氧化碳浓度ppm"
    point_temp_column = _first_existing(
        ["数字温度计温度C_平均值", "数字温度计温度C", "温箱目标温度C"],
        points,
    )
    point_set_temp_column = _first_existing(["温箱目标温度C"], points)
    pressure_column = _first_existing(
        ["数字压力计压力hPa_平均值", "压力控制器压力hPa_平均值", "目标压力hPa"],
        points,
    )
    if not point_temp_column or not pressure_column:
        raise ValueError("Unable to resolve point temperature/pressure columns from points CSV")

    work = points.copy()
    work[point_row_column] = _safe_numeric_series(work[point_row_column])
    work[target_column] = _safe_numeric_series(work[target_column])
    stage_mask = work[stage_column].fillna("").astype(str).str.lower() == "co2"
    work = work[stage_mask].copy()

    fix_rows: list[dict[str, Any]] = []
    for analyzer_id, spec in sorted(specs.items()):
        model_path = _find_run_artifact(run_dir, f"co2_{analyzer_id}_ratio_poly_fit_*.json")
        model_payload = _read_json(model_path)
        coefficients = dict(model_payload.get("simplified_coefficients", {}))
        if not coefficients:
            raise ValueError(f"Missing simplified_coefficients in {model_path}")

        raw_ratio_column = f"气体分析仪{spec.slot}_二氧化碳比值原始值_平均值"
        filt_ratio_column = f"气体分析仪{spec.slot}_二氧化碳比值滤波后_平均值"
        cavity_column = f"气体分析仪{spec.slot}_温度箱温度C_平均值"
        shell_column = f"气体分析仪{spec.slot}_机壳温度C_平均值"
        native_column = f"气体分析仪{spec.slot}_二氧化碳浓度ppm_平均值"
        for required_column in (raw_ratio_column, filt_ratio_column, native_column):
            if required_column not in work.columns:
                raise ValueError(f"Missing required point column: {required_column}")

        for _, row in work.iterrows():
            raw_ratio = _safe_float(row.get(raw_ratio_column))
            filt_ratio = _safe_float(row.get(filt_ratio_column))
            pressure_hpa = _safe_float(row.get(pressure_column))
            point_temp_c = _safe_float(row.get(point_temp_column))
            if pressure_hpa is None or point_temp_c is None:
                continue

            selected_ratio = raw_ratio if raw_ratio is not None else filt_ratio
            ratio_source_mode = "raw_first_with_fallback" if raw_ratio is not None else "filt_fallback"
            if selected_ratio is None:
                continue

            temp_input_value, temp_input_source = _point_temp_input(
                row,
                spec=spec,
                cavity_column=cavity_column,
                shell_column=shell_column,
                point_temp_column=point_temp_column,
                point_set_temp_column=point_set_temp_column,
            )
            if temp_input_value is None:
                continue

            base_pred_ppm = _evaluate_ratio_poly(coefficients, selected_ratio, point_temp_c, pressure_hpa)
            applied_temp_effect_ppm = spec.loaded_temp_intercept + spec.loaded_temp_slope * temp_input_value
            fixed_native_ppm = base_pred_ppm + applied_temp_effect_ppm

            fix_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "slot": spec.slot,
                    "point_row": int(float(row.get(point_row_column))),
                    "point_title": str(row.get(point_title_column) or ""),
                    "target_ppm": _safe_float(row.get(target_column)),
                    "ratio_source_mode": ratio_source_mode,
                    "ratio_raw_mean": raw_ratio,
                    "ratio_filt_mean": filt_ratio,
                    "selected_ratio_value": selected_ratio,
                    "point_temp_c": point_temp_c,
                    "temp_input_source": temp_input_source,
                    "temp_input_value_c": temp_input_value,
                    "loaded_temp_intercept": spec.loaded_temp_intercept,
                    "loaded_temp_slope": spec.loaded_temp_slope,
                    "applied_temperature_effect_ppm": applied_temp_effect_ppm,
                    "base_pred_ppm": base_pred_ppm,
                    "native_original_point_mean_ppm": _safe_float(row.get(native_column)),
                    "native_fixed_point_mean_ppm": fixed_native_ppm,
                    "native_point_delta_ppm": fixed_native_ppm - (_safe_float(row.get(native_column)) or 0.0),
                    "coefficient_order_before_flag": spec.coefficient_order_match_flag,
                    "coefficient_order_after": "temp_intercept,temp_slope",
                    "coefficient_sign_before_flag": spec.coefficient_sign_match_flag,
                    "scale_ratio_loaded_to_native_before": spec.scale_ratio_loaded_to_native,
                    "denominator_or_range_expected": spec.denominator_expected,
                    "denominator_or_range_observed": spec.denominator_observed,
                    "before_normalization_proxy": spec.before_normalization_proxy,
                    "after_normalization_proxy": spec.after_normalization_proxy,
                    "expected_temp_normalization_formula": spec.expected_formula,
                    "recommended_fix_formula": spec.recommended_formula,
                    "recommended_fix_stage": spec.recommended_stage,
                    "coefficient_mapping_table": spec.coefficient_mapping_table,
                }
            )

    frame = pd.DataFrame(fix_rows)
    if frame.empty:
        raise ValueError("No point-level temperature fixes were constructed from the bundle")
    return frame.sort_values(["analyzer_id", "point_row"], ignore_index=True)


def _patch_samples(samples: pd.DataFrame, point_fix: pd.DataFrame) -> pd.DataFrame:
    stage_column = "流程阶段"
    point_row_column = "校准点行号"
    patched = samples.copy()
    patched[point_row_column] = _safe_numeric_series(patched[point_row_column])
    co2_mask = patched[stage_column].fillna("").astype(str).str.lower() == "co2"

    for analyzer_id, subset in point_fix.groupby("analyzer_id", dropna=False):
        slot = int(subset["slot"].iloc[0])
        sample_column = f"气体分析仪{slot}_二氧化碳浓度ppm"
        if sample_column not in patched.columns:
            continue
        before_column = f"{sample_column}_修复前"
        if before_column not in patched.columns:
            patched[before_column] = patched[sample_column]
        fix_map = {
            int(row.point_row): float(row.native_fixed_point_mean_ppm)
            for row in subset.itertuples(index=False)
        }
        row_values = patched[point_row_column].map(lambda value: fix_map.get(int(value)) if pd.notna(value) and int(value) in fix_map else math.nan)
        apply_mask = co2_mask & row_values.notna()
        patched.loc[apply_mask, sample_column] = row_values.loc[apply_mask]

        if slot == 1 and "二氧化碳浓度ppm" in patched.columns:
            unprefixed_before = "二氧化碳浓度ppm_修复前"
            if unprefixed_before not in patched.columns:
                patched[unprefixed_before] = patched["二氧化碳浓度ppm"]
            patched.loc[apply_mask, "二氧化碳浓度ppm"] = row_values.loc[apply_mask]
    return patched


def _patch_point_like_frame(frame: pd.DataFrame, point_fix: pd.DataFrame) -> pd.DataFrame:
    patched = frame.copy()
    point_row_column = "校准点行号"
    stage_column = "流程阶段"
    patched[point_row_column] = _safe_numeric_series(patched[point_row_column])
    co2_mask = patched[stage_column].fillna("").astype(str).str.lower() == "co2" if stage_column in patched.columns else pd.Series(True, index=patched.index)

    for analyzer_id, subset in point_fix.groupby("analyzer_id", dropna=False):
        slot = int(subset["slot"].iloc[0])
        mean_column = f"气体分析仪{slot}_二氧化碳浓度ppm_平均值"
        if mean_column not in patched.columns:
            continue
        before_column = f"{mean_column}_修复前"
        if before_column not in patched.columns:
            patched[before_column] = patched[mean_column]
        fix_map = {
            int(row.point_row): float(row.native_fixed_point_mean_ppm)
            for row in subset.itertuples(index=False)
        }
        row_values = patched[point_row_column].map(lambda value: fix_map.get(int(value)) if pd.notna(value) and int(value) in fix_map else math.nan)
        apply_mask = co2_mask & row_values.notna()
        patched.loc[apply_mask, mean_column] = row_values.loc[apply_mask]

        if slot == 1:
            for generic_column in ("二氧化碳浓度ppm_平均值", "二氧化碳平均值", "二氧化碳平均值(主分析仪或首台可用)"):
                if generic_column not in patched.columns:
                    continue
                generic_before = f"{generic_column}_修复前"
                if generic_before not in patched.columns:
                    patched[generic_before] = patched[generic_column]
                patched.loc[apply_mask, generic_column] = row_values.loc[apply_mask]
    return patched


def _write_observability(
    run_dir: Path,
    *,
    version_tag: str,
    point_fix: pd.DataFrame,
    specs: dict[str, AnalyzerTempFixSpec],
    step31_path: Path,
    source_bundle_path: Path,
) -> dict[str, Path]:
    point_patch_path = run_dir / "runtime_temp_fix_point_patch.csv"
    point_fix.to_csv(point_patch_path, index=False, encoding="utf-8-sig")

    summary_rows: list[dict[str, Any]] = []
    for analyzer_id, subset in point_fix.groupby("analyzer_id", dropna=False):
        spec = specs[str(analyzer_id)]
        summary_rows.append(
            {
                "version_tag": version_tag,
                "fix_flag": True,
                "analyzer_id": analyzer_id,
                "loaded_temp_intercept": spec.loaded_temp_intercept,
                "loaded_temp_slope": spec.loaded_temp_slope,
                "temp_column": spec.temp_column,
                "temp_source": spec.temp_source,
                "coefficient_order_before_flag": spec.coefficient_order_match_flag,
                "coefficient_order_after": "temp_intercept,temp_slope",
                "coefficient_sign_before_flag": spec.coefficient_sign_match_flag,
                "scale_ratio_loaded_to_native_before": spec.scale_ratio_loaded_to_native,
                "denominator_or_range_expected": spec.denominator_expected,
                "denominator_or_range_observed": spec.denominator_observed,
                "before_normalization_proxy": spec.before_normalization_proxy,
                "after_normalization_proxy": spec.after_normalization_proxy,
                "temp_effect_mean_ppm": float(pd.to_numeric(subset["applied_temperature_effect_ppm"], errors="coerce").mean()),
                "temp_effect_min_ppm": float(pd.to_numeric(subset["applied_temperature_effect_ppm"], errors="coerce").min()),
                "temp_effect_max_ppm": float(pd.to_numeric(subset["applied_temperature_effect_ppm"], errors="coerce").max()),
                "native_delta_mean_ppm": float(pd.to_numeric(subset["native_point_delta_ppm"], errors="coerce").mean()),
                "native_delta_min_ppm": float(pd.to_numeric(subset["native_point_delta_ppm"], errors="coerce").min()),
                "native_delta_max_ppm": float(pd.to_numeric(subset["native_point_delta_ppm"], errors="coerce").max()),
                "point_count": int(len(subset)),
            }
        )
    summary_path = run_dir / "runtime_temp_fix_observability_summary.csv"
    pd.DataFrame(summary_rows).sort_values("analyzer_id", ignore_index=True).to_csv(
        summary_path,
        index=False,
        encoding="utf-8-sig",
    )

    checklist_rows = _load_csv_rows(step31_path)
    checklist_row = checklist_rows[0] if checklist_rows else {}
    summary_md = run_dir / "runtime_temp_fix_summary.md"
    summary_md.write_text(
        "\n".join(
            [
                "# Runtime Temp Fix Summary",
                "",
                f"- version_tag: `{version_tag}`",
                f"- fix_flag: `True`",
                f"- source_bundle_path: `{source_bundle_path}`",
                f"- step_31_reference: `{step31_path}`",
                f"- root_cause_stage: `{checklist_row.get('root_cause_stage', 'loaded_to_native')}`",
                f"- recommended_fix_formula: `{checklist_row.get('recommended_fix_formula', '')}`",
                f"- recommended_fix_stage: `{checklist_row.get('recommended_fix_stage', '')}`",
                f"- stable_surpass_old_required_flag: `{checklist_row.get('stable_surpass_old_required_flag', 'True')}`",
                "",
                "This bundle only updates the native CO2 ppm surface and adds observability artifacts.",
                "No source policy, water, pressure, family, filtering, or GA01 sidecar logic was changed.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "point_patch_csv": point_patch_path,
        "summary_csv": summary_path,
        "summary_md": summary_md,
    }


def _update_runtime_snapshot(
    snapshot_path: Path,
    *,
    version_tag: str,
    observability_paths: dict[str, Path],
    source_bundle_path: Path,
    step25_path: Path,
    step31_path: Path,
) -> None:
    snapshot = _read_json(snapshot_path)
    snapshot["runtime_temp_fix"] = {
        "version_tag": version_tag,
        "fix_flag": True,
        "source_bundle_path": str(source_bundle_path),
        "step_25_spec_path": str(step25_path),
        "step_31_checklist_path": str(step31_path),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "generated_files": {name: path.name for name, path in observability_paths.items()},
        "scope_note": "temperature normalization only; no source/water/pressure/family/filtering changes",
    }
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_bundle_zip(run_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(run_dir.rglob("*")):
            if path.is_dir():
                continue
            archive.write(path, arcname=str(path.relative_to(run_dir.parent)).replace("\\", "/"))


def apply_runtime_temp_fix_to_bundle(
    *,
    bundle_path: str | Path,
    debugger_output_dir: str | Path,
    output_root: str | Path | None = None,
    version_tag: str = DEFAULT_VERSION_TAG,
) -> dict[str, Any]:
    source_bundle_path = Path(bundle_path).resolve()
    debugger_output_path = Path(debugger_output_dir).resolve()
    step17_path = debugger_output_path / "step_17_native_temp_gain_sign_audit_summary.csv"
    step19_path = debugger_output_path / "step_19_temp_coefficient_provenance_summary.csv"
    step25_path = debugger_output_path / "step_25_runtime_temp_fix_handoff_spec.csv"
    step31_path = debugger_output_path / "step_31_runtime_handoff_acceptance_checklist.csv"
    for path in (step17_path, step19_path, step25_path, step31_path):
        if not path.exists():
            raise FileNotFoundError(f"Missing debugger handoff artifact: {path}")

    specs = _load_analyzer_specs(
        step17_summary_path=step17_path,
        step19_summary_path=step19_path,
        step25_spec_path=step25_path,
    )

    if output_root is None:
        output_root_path = source_bundle_path.parent / "runtime_temp_fix" / version_tag
    else:
        output_root_path = Path(output_root).resolve()
    output_root_path.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="runtime_temp_fix_") as temp_dir_text:
        temp_dir = Path(temp_dir_text)
        extracted_root, _extract_base = _discover_bundle_root(source_bundle_path, temp_dir)
        run_dir_name = extracted_root.name
        fixed_run_dir = output_root_path / run_dir_name
        if fixed_run_dir.exists():
            shutil.rmtree(fixed_run_dir)
        shutil.copytree(extracted_root, fixed_run_dir)

    point_fix = _load_point_fix_frame(fixed_run_dir, specs)

    samples_path = _find_run_artifact(fixed_run_dir, "samples_*.csv")
    points_path = _find_run_artifact(fixed_run_dir, "points_*.csv")
    snapshot_path = fixed_run_dir / "runtime_config_snapshot.json"
    points_readable_paths = sorted(fixed_run_dir.glob("points_readable_*.csv"))

    samples = pd.read_csv(samples_path, encoding="utf-8-sig")
    points = pd.read_csv(points_path, encoding="utf-8-sig")
    samples_patched = _patch_samples(samples, point_fix)
    points_patched = _patch_point_like_frame(points, point_fix)
    samples_patched.to_csv(samples_path, index=False, encoding="utf-8-sig")
    points_patched.to_csv(points_path, index=False, encoding="utf-8-sig")
    for readable_path in points_readable_paths:
        readable = pd.read_csv(readable_path, encoding="utf-8-sig")
        readable_patched = _patch_point_like_frame(readable, point_fix)
        readable_patched.to_csv(readable_path, index=False, encoding="utf-8-sig")

    observability_paths = _write_observability(
        fixed_run_dir,
        version_tag=version_tag,
        point_fix=point_fix,
        specs=specs,
        step31_path=step31_path,
        source_bundle_path=source_bundle_path,
    )
    _update_runtime_snapshot(
        snapshot_path,
        version_tag=version_tag,
        observability_paths=observability_paths,
        source_bundle_path=source_bundle_path,
        step25_path=step25_path,
        step31_path=step31_path,
    )

    fixed_bundle_zip = output_root_path / f"{run_dir_name}.zip"
    _write_bundle_zip(fixed_run_dir, fixed_bundle_zip)

    return {
        "source_bundle_path": str(source_bundle_path),
        "debugger_output_dir": str(debugger_output_path),
        "fixed_run_dir": str(fixed_run_dir),
        "fixed_bundle_zip": str(fixed_bundle_zip),
        "samples_path": str(samples_path),
        "points_path": str(points_path),
        "points_readable_paths": [str(path) for path in points_readable_paths],
        "runtime_snapshot_path": str(snapshot_path),
        "observability_paths": {name: str(path) for name, path in observability_paths.items()},
        "version_tag": version_tag,
        "analyzers": sorted(specs.keys()),
        "point_fix_rows": int(len(point_fix)),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="runtime-temp-fix-bundle",
        description="Apply the step-25 runtime temperature normalization fix to a production bundle.",
    )
    parser.add_argument("bundle_path", help="Path to the source production bundle zip or extracted directory.")
    parser.add_argument(
        "--debugger-output-dir",
        required=True,
        help="Directory containing step_17 / step_19 / step_25 / step_31 debugger artifacts.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Directory that will receive the fixed run directory and fixed bundle zip.",
    )
    parser.add_argument(
        "--version-tag",
        default=DEFAULT_VERSION_TAG,
        help=f"Version tag written into runtime observability metadata (default: {DEFAULT_VERSION_TAG}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = apply_runtime_temp_fix_to_bundle(
        bundle_path=args.bundle_path,
        debugger_output_dir=args.debugger_output_dir,
        output_root=args.output_root,
        version_tag=args.version_tag,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
