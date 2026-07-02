"""Export V1.5 no-write temperature-channel review artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..validation.v1_5_temperature_channel_review import (
    DEFAULT_EXCLUDED_DEVICE_IDS,
    DEFAULT_TARGET_DEVICE_IDS,
    export_temperature_channel_review,
)


def _split_ids(text: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if text is None:
        return default
    values = [item.strip().zfill(3) for item in text.replace(";", ",").split(",") if item.strip()]
    return tuple(values) or default


def _split_ids_allow_empty(text: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if text is None:
        return default
    if not text.strip():
        return ()
    return _split_ids(text, default)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build offline no-write SENCO7/SENCO8 temperature-channel review from V1.5 "
            "full-temperature point artifacts."
        )
    )
    parser.add_argument(
        "--h2o-points-parent",
        type=Path,
        default=None,
        help="Directory containing p*_h2o point folders.",
    )
    parser.add_argument(
        "--open-flow-points-parent",
        type=Path,
        default=None,
        help="Directory containing V1.5 open-flow p* point folders with samples_machine_readable.csv.",
    )
    parser.add_argument(
        "--snapshot-run-dir",
        type=Path,
        action="append",
        default=[],
        help="validate_dry_collect run directory containing samples_*.csv and temperature IO evidence.",
    )
    parser.add_argument(
        "--co2-residual-csv",
        type=Path,
        default=None,
        help="Optional CO2 candidate residual CSV used for temperature-impact diagnostics.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("logs/v1_5_temperature_channel_review_20260531/h2o_fulltemp_digital_thermometer_r1"),
        help="Output directory for review artifacts.",
    )
    parser.add_argument(
        "--target-device-ids",
        default=",".join(DEFAULT_TARGET_DEVICE_IDS),
        help="Comma-separated analyzer device IDs to review.",
    )
    parser.add_argument(
        "--excluded-device-ids",
        default=",".join(DEFAULT_EXCLUDED_DEVICE_IDS),
        help="Comma-separated device IDs to include as rejected observations only.",
    )
    parser.add_argument(
        "--no-excluded-device-ids",
        action="store_true",
        help="Do not mark any analyzer IDs as excluded in this review.",
    )
    parser.add_argument(
        "--no-command-preview",
        action="store_true",
        help="Do not include SENCO7/8 command strings in generated artifacts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    payload = export_temperature_channel_review(
        args.output_dir,
        h2o_points_parent=args.h2o_points_parent,
        open_flow_points_parent=args.open_flow_points_parent,
        snapshot_run_dirs=tuple(args.snapshot_run_dir or ()),
        co2_residual_csv=args.co2_residual_csv,
        target_device_ids=_split_ids(args.target_device_ids, DEFAULT_TARGET_DEVICE_IDS),
        excluded_device_ids=()
        if args.no_excluded_device_ids
        else _split_ids_allow_empty(args.excluded_device_ids, DEFAULT_EXCLUDED_DEVICE_IDS),
        export_commands=not args.no_command_preview,
    )
    summary = {
        "output_dir": str(args.output_dir),
        "observation_count": len(payload["observations"]),
        "temperature_result_count": len(payload["temperature_results"]),
        "summary_count": len(payload["summary_rows"]),
        "impact_count": len(payload["impact_rows"]),
        "opens_com_ports": False,
        "writes_coefficients": False,
        "controls_water_or_gas_routes": False,
        "physical_meaning": (
            "SENCO7/SENCO8 are independent temperature input channels used by the "
            "CO2/H2O ratio-temperature model. This review uses existing digital "
            "thermometer evidence only."
        ),
        "paths": {key: str(value) for key, value in payload["paths"].items()},
        "h2o_points_parent": str(args.h2o_points_parent) if args.h2o_points_parent else None,
        "open_flow_points_parent": str(args.open_flow_points_parent) if args.open_flow_points_parent else None,
        "snapshot_run_dirs": [str(path) for path in args.snapshot_run_dir or []],
    }
    (args.output_dir / "temperature_channel_review_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
