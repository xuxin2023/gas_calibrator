"""Export a no-write V1.5 H2O dry-anchor bridge review package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_dry_anchor_bridge_review import (
    H2ODryAnchorBridgeConfig,
    write_h2o_dry_anchor_bridge_review,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export no-write H2O dry-anchor bridge review from V1.5 wet H2O and gas-route "
            "dry-point artifacts."
        )
    )
    parser.add_argument(
        "--wet-run-dir",
        required=True,
        help="H2O wet-route run directory or parent directory containing p*_h2o points.",
    )
    parser.add_argument(
        "--dry-anchor-run-dir",
        action="append",
        default=[],
        help=(
            "CO2/gas-route run directory containing zero-gas dry points to evaluate as H2O "
            "low-water bridge evidence. Can be repeated."
        ),
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for bridge review artifacts.")
    parser.add_argument("--min-points", type=int, default=8)
    parser.add_argument("--min-wet-points", type=int, default=3)
    parser.add_argument("--wet-fit-max-abs-error-mmol", type=float, default=0.5)
    parser.add_argument("--design-max-relative-error-pct", type=float, default=2.0)
    parser.add_argument(
        "--relative-error-min-reference-mmol",
        type=float,
        default=2.0,
        help="Only compute wet-fit relative H2O percent error when reference H2O is above this level.",
    )
    parser.add_argument(
        "--bridge-max-abs-error-mmol",
        type=float,
        default=0.25,
        help="Absolute H2O agreement limit for a dry anchor to be bridge-compatible.",
    )
    parser.add_argument(
        "--bridge-max-relative-error-pct",
        type=float,
        default=2.0,
        help="Relative H2O agreement limit for a dry anchor to be bridge-compatible.",
    )
    parser.add_argument(
        "--bridge-relative-error-min-reference-mmol",
        type=float,
        default=2.0,
        help="Only compute dry-anchor bridge relative percent error above this H2O level.",
    )
    parser.add_argument(
        "--dry-anchor-min-temp-c",
        type=float,
        default=None,
        help="Optional minimum temperature setpoint for gas-route dry anchors.",
    )
    parser.add_argument(
        "--dry-anchor-max-temp-c",
        type=float,
        default=None,
        help="Optional maximum temperature setpoint for gas-route dry anchors.",
    )
    parser.add_argument(
        "--allow-pressure-qc-failed-device-id",
        action="append",
        default=[],
        help=(
            "Analyzer device ID whose pressure-QC-failed MODE2 samples may still provide H2O "
            "ratio/temperature evidence for component fitting. This does not mark pressure accepted."
        ),
    )
    parser.add_argument(
        "--fit-temperature-source",
        choices=("analyzer_chamber", "digital_thermometer"),
        default="digital_thermometer",
        help=(
            "Temperature source used by the bridge fit. Use digital_thermometer when chamber "
            "temperature coefficients were repaired after acquisition."
        ),
    )
    parser.add_argument(
        "--fit-objective",
        choices=("absolute_mmol", "sqrt_relative_mmol_floor", "relative_mmol_floor"),
        default="relative_mmol_floor",
        help="Wet H2O SENCO2/SENCO4 objective used before evaluating dry anchors.",
    )
    parser.add_argument(
        "--component-snapshot-json",
        default=None,
        help="Optional read-only GETCO component snapshot JSON for SENCO6 layer review.",
    )
    parser.add_argument(
        "--no-require-component-snapshot-for-layer-review",
        action="store_true",
        default=False,
        help=(
            "Do not require a GETCO6 snapshot for layer-review warnings. The bridge remains no-write."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_component_snapshot(path: str | None) -> dict[str, object]:
    if not path:
        return {}
    snapshot_path = Path(path)
    return json.loads(snapshot_path.read_text(encoding="utf-8-sig"))


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = H2ODryAnchorBridgeConfig(
            min_points=int(args.min_points),
            min_wet_points=int(args.min_wet_points),
            fit_temperature_source=str(args.fit_temperature_source),
            fit_objective=str(args.fit_objective),
            relative_error_min_reference_mmol=float(args.relative_error_min_reference_mmol),
            wet_fit_max_abs_error_mmol=float(args.wet_fit_max_abs_error_mmol),
            design_max_relative_error_pct=float(args.design_max_relative_error_pct),
            bridge_max_abs_error_mmol=float(args.bridge_max_abs_error_mmol),
            bridge_max_relative_error_pct=float(args.bridge_max_relative_error_pct),
            bridge_relative_error_min_reference_mmol=float(args.bridge_relative_error_min_reference_mmol),
            allow_pressure_qc_failed_device_ids=tuple(args.allow_pressure_qc_failed_device_id or ()),
            component_snapshot=_load_component_snapshot(args.component_snapshot_json),
            require_component_snapshot_for_layer_review=not bool(
                args.no_require_component_snapshot_for_layer_review
            ),
            dry_anchor_min_temp_c=args.dry_anchor_min_temp_c,
            dry_anchor_max_temp_c=args.dry_anchor_max_temp_c,
        )
        outputs = write_h2o_dry_anchor_bridge_review(
            wet_run_dir=args.wet_run_dir,
            dry_anchor_run_dirs=tuple(args.dry_anchor_run_dir or ()),
            output_dir=args.output_dir,
            cfg=cfg,
        )
    except Exception as exc:
        print(f"V1.5 H2O dry-anchor bridge review export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
