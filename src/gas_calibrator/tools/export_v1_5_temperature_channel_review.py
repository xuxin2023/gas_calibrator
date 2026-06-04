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
    if not text:
        return default
    values = [item.strip().zfill(3) for item in text.replace(";", ",").split(",") if item.strip()]
    return tuple(values) or default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build offline no-write SENCO7/SENCO8 temperature-channel review from V1.5 "
            "full-temperature H2O point artifacts."
        )
    )
    parser.add_argument(
        "--h2o-points-parent",
        type=Path,
        default=Path("logs/v1_5_formal_h2o_multitemp_no_write_20260530"),
        help="Directory containing p*_h2o point folders.",
    )
    parser.add_argument(
        "--co2-residual-csv",
        type=Path,
        default=Path(
            "logs/co2_fulltemp_all_eligible_exclude023_100_candidate_20260531/"
            "candidate_temp_terms_all_points_reference_h2o_no_pressure_r3/"
            "candidate_fit_residuals.csv"
        ),
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
        co2_residual_csv=args.co2_residual_csv,
        target_device_ids=_split_ids(args.target_device_ids, DEFAULT_TARGET_DEVICE_IDS),
        excluded_device_ids=_split_ids(args.excluded_device_ids, DEFAULT_EXCLUDED_DEVICE_IDS),
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
    }
    (args.output_dir / "temperature_channel_review_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
