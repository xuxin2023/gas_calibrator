"""CLI for the offline absorbance debugger."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .app import run_debugger
from .options import (
    normalize_absorbance_order_mode,
    normalize_model_selection_strategy,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
    parse_numeric_csv,
)


def _csv_list(text: str) -> tuple[str, ...]:
    return tuple(part.strip().upper() for part in str(text).split(",") if part.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconstruct the CO2 absorbance chain from a historical run bundle.",
    )
    parser.add_argument("input_path", help="Path to run_xxx.zip or an extracted run directory.")
    parser.add_argument(
        "--output-dir",
        help="Directory for generated CSV, plots, Excel files, and reports. Defaults to <input>/output/absorbance_debugger/<stem>.",
    )
    parser.add_argument(
        "--analyzers",
        default="GA01,GA02,GA03",
        help="Comma-separated analyzer whitelist for main results.",
    )
    parser.add_argument(
        "--warning-only-analyzers",
        default="GA04",
        help="Comma-separated analyzers to detect and warn about but exclude from main fits.",
    )
    parser.add_argument("--enable-base-final", action="store_true", help="Enable the optional absorbance-domain base/final branch.")
    parser.add_argument(
        "--ratio-source",
        default="raw",
        help="Default comparison branch ratio source: raw or filt.",
    )
    parser.add_argument(
        "--temperature-source",
        default="corr",
        help="Default comparison branch temperature source: std or corr.",
    )
    parser.add_argument(
        "--pressure-source",
        default="corr",
        help="Default comparison branch pressure source: std or corr.",
    )
    parser.add_argument(
        "--absorbance-order-mode",
        default="samplewise_log_first",
        help="Primary absorbance order mode: samplewise_log_first, mean_first_log, or compare_both.",
    )
    parser.add_argument(
        "--model-selection-strategy",
        default="auto",
        help="Absorbance model validation strategy: auto, grouped_loo, or grouped_kfold.",
    )
    parser.add_argument(
        "--invalid-pressure-targets-hpa",
        default="500",
        help="Comma-separated pressure bins that are legacy-invalid and must be excluded from the main chain.",
    )
    parser.add_argument(
        "--invalid-pressure-tolerance-hpa",
        type=float,
        default=30.0,
        help="Tolerance used to detect legacy-invalid pressure bins.",
    )
    parser.add_argument(
        "--disable-hard-invalid-pressure-exclude",
        action="store_true",
        help="Keep legacy-invalid pressure bins for diagnostics instead of hard-excluding them from the main chain.",
    )
    parser.add_argument(
        "--full-data-main-conclusion",
        action="store_true",
        help="Use full-data instead of valid-only as the main conclusion surface.",
    )
    parser.add_argument(
        "--no-composite-score",
        action="store_true",
        help="Disable the weighted composite score and fall back to validation RMSE selection.",
    )
    parser.add_argument("--eps", type=float, default=1.0e-9, help="Lower clamp used in logarithm inputs.")
    parser.add_argument("--p-min-hpa", type=float, default=100.0, help="Lower clamp used for pressure in hPa.")
    parser.add_argument("--p-ref-hpa", type=float, default=1013.25, help="Reference pressure in hPa.")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not delete an existing output directory before writing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_debugger(
        args.input_path,
        output_dir=args.output_dir,
        analyzers=_csv_list(args.analyzers),
        warning_only_analyzers=_csv_list(args.warning_only_analyzers),
        enable_base_final=bool(args.enable_base_final),
        ratio_source=normalize_ratio_source(args.ratio_source),
        temperature_source=normalize_temp_source(args.temperature_source),
        pressure_source=normalize_pressure_source(args.pressure_source),
        absorbance_order_mode=normalize_absorbance_order_mode(args.absorbance_order_mode),
        model_selection_strategy=normalize_model_selection_strategy(args.model_selection_strategy),
        enable_composite_score=not bool(args.no_composite_score),
        invalid_pressure_targets_hpa=parse_numeric_csv(args.invalid_pressure_targets_hpa),
        invalid_pressure_tolerance_hpa=float(args.invalid_pressure_tolerance_hpa),
        invalid_pressure_mode="diagnostic_only" if bool(args.disable_hard_invalid_pressure_exclude) else "hard_exclude",
        use_valid_only_main_conclusion=not bool(args.full_data_main_conclusion),
        eps=float(args.eps),
        p_min_hpa=float(args.p_min_hpa),
        p_ref_hpa=float(args.p_ref_hpa),
        overwrite_output=not bool(args.no_overwrite),
    )
    summary = {
        "output_dir": str(Path(result["output_dir"]).resolve()),
        "validation": result["validation_table"].to_dict(orient="records"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0
