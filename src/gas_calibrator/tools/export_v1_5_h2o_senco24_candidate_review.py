"""Export a no-write V1.5 H2O SENCO2/SENCO4 candidate review package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.h2o_senco24_candidate_review import (
    H2OSenco24CandidateConfig,
    write_h2o_senco24_candidate_report,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export no-write H2O SENCO2/SENCO4 candidate review from V1.5 open-flow artifacts."
    )
    parser.add_argument("--run-dir", required=True, help="H2O run directory or parent directory containing p*_h2o points.")
    parser.add_argument(
        "--additional-h2o-run-dir",
        action="append",
        default=[],
        help=(
            "Optional extra H2O run or completed point directory to merge into the no-write review. "
            "Can be repeated. This is useful when a device has valid H2O evidence in a separate run."
        ),
    )
    parser.add_argument(
        "--dry-anchor-run-dir",
        action="append",
        default=[],
        help=(
            "Optional CO2/gas-route run directory containing zero-gas points used only as H2O dry-gas "
            "low-water anchors. Can be repeated. Targets remain dewpoint/pressure-derived, not zeroed."
        ),
    )
    parser.add_argument(
        "--dry-anchor-min-temp-c",
        type=float,
        default=None,
        help="Optional minimum temperature setpoint for CO2-route dry-gas anchors.",
    )
    parser.add_argument(
        "--dry-anchor-max-temp-c",
        type=float,
        default=None,
        help="Optional maximum temperature setpoint for CO2-route dry-gas anchors.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for candidate review artifacts.")
    parser.add_argument(
        "--database-sidecar-json",
        default=None,
        help="Optional JSON path for database sidecar metadata. Defaults under output-dir.",
    )
    parser.add_argument("--min-points", type=int, default=8)
    parser.add_argument(
        "--min-wet-points",
        type=int,
        default=3,
        help=(
            "Minimum true wet H2O open-flow points required per device. Dry-gas anchors can constrain "
            "the low-water baseline but cannot replace wet humidity-response evidence."
        ),
    )
    parser.add_argument("--max-condition-number", type=float, default=1.0e8)
    parser.add_argument("--fit-max-abs-error-mmol", type=float, default=0.5)
    parser.add_argument("--design-max-relative-error-pct", type=float, default=2.0)
    parser.add_argument(
        "--relative-error-min-reference-mmol",
        type=float,
        default=2.0,
        help="Only compute relative H2O percent error when the reference H2O is above this level.",
    )
    parser.add_argument(
        "--exclude-device-id",
        action="append",
        default=[],
        help="Analyzer device ID to exclude from candidate generation. Can be repeated.",
    )
    parser.add_argument(
        "--manual-device-block",
        action="append",
        default=[],
        help="Manual device block in DEVICE_ID=reason form, e.g. 100=firmware_upgrade_required.",
    )
    parser.add_argument(
        "--manual-point-block",
        action="append",
        default=[],
        help=(
            "Manual point block in DEVICE_ID:POINT_RUN_ID=reason or DEVICE_ID:POINT_ID=reason form. "
            "The point is preserved as rejected evidence and excluded from fitting."
        ),
    )
    parser.add_argument(
        "--component-snapshot-json",
        default=None,
        help="Optional read-only GETCO component snapshot JSON, e.g. old_component_coefficients_snapshot.json.",
    )
    parser.add_argument(
        "--require-component-snapshot-for-layer-review",
        action="store_true",
        default=False,
        help=(
            "Require a read-only GETCO6 snapshot before marking SENCO2/SENCO4 as write candidates. "
            "Use this for formal reviews when SENCO6 may not have been neutralized before acquisition."
        ),
    )
    parser.add_argument(
        "--postwrite-verified-device-id",
        action="append",
        default=[],
        help=(
            "Analyzer device ID whose historical pre-write final-output issue is resolved by attached "
            "controlled write/readback and independent H2O verification evidence. Can be repeated."
        ),
    )
    parser.add_argument(
        "--postwrite-verification-artifact",
        action="append",
        default=[],
        help="Path to post-write verification evidence artifact, e.g. summary markdown. Can be repeated.",
    )
    parser.add_argument(
        "--allow-pressure-qc-failed-device-id",
        action="append",
        default=[],
        help=(
            "Analyzer device ID whose pressure-QC-failed MODE2 samples may still provide H2O ratio/"
            "temperature evidence for component fitting. This does not mark pressure accepted."
        ),
    )
    parser.add_argument(
        "--fit-temperature-source",
        choices=("analyzer_chamber", "digital_thermometer"),
        default="analyzer_chamber",
        help=(
            "Temperature source used by the H2O fit. Use digital_thermometer only for a "
            "post-temperature-repair review when analyzer chamber T was known bad during acquisition."
        ),
    )
    parser.add_argument(
        "--fit-objective",
        choices=(
            "absolute_mmol",
            "sqrt_relative_mmol_floor",
            "relative_mmol_floor",
            "minimax_relative_mmol_floor_monotonic",
        ),
        default="absolute_mmol",
        help=(
            "Fit objective for SENCO2/SENCO4. Use relative_mmol_floor when the review "
            "must minimize squared relative H2O error above the configured reference floor. Use "
            "minimax_relative_mmol_floor_monotonic to minimize the maximum relative error while "
            "enforcing the physical decreasing concentration-versus-ratio response."
        ),
    )
    parser.add_argument(
        "--factory-signal-health-summary-csv",
        default=None,
        help=(
            "Optional factory-signal health summary CSV. When provided, devices whose gate is not "
            "pass_factory_signal_health are blocked from H2O candidate write review."
        ),
    )
    parser.add_argument(
        "--state-transfer-summary-csv",
        default=None,
        help=(
            "Optional H2O state-transfer CSV, for example "
            "h2o_s24_post_s6_state_delta_by_device_point.csv. When provided, devices whose "
            "S2/S4 raw replay movement cannot transfer between calibration and verification states "
            "are blocked from write review."
        ),
    )
    parser.add_argument(
        "--state-transfer-max-raw-excess-shift-mmol",
        type=float,
        default=0.1,
        help=(
            "Maximum allowed absolute raw S2/S4 replay movement in excess of the live reference "
            "H2O movement between states."
        ),
    )
    parser.add_argument(
        "--state-transfer-max-post-s6-relative-error-pct",
        type=float,
        default=2.0,
        help="Maximum allowed post-S6 state-transfer relative error percentage.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _parse_manual_blocks(values: Iterable[str]) -> dict[str, str]:
    blocks: dict[str, str] = {}
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        if "=" in text:
            device_id, reason = text.split("=", 1)
        elif ":" in text:
            device_id, reason = text.split(":", 1)
        else:
            raise ValueError("--manual-device-block must use DEVICE_ID=reason")
        device_id = device_id.strip()
        reason = reason.strip()
        if not device_id or not reason:
            raise ValueError("--manual-device-block requires both DEVICE_ID and reason")
        blocks[device_id] = reason
    return blocks


def _parse_manual_point_blocks(values: Iterable[str]) -> dict[str, str]:
    blocks: dict[str, str] = {}
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        if "=" in text:
            key, reason = text.split("=", 1)
        else:
            raise ValueError("--manual-point-block must use DEVICE_ID:POINT=reason")
        key = key.strip()
        reason = reason.strip()
        if ":" not in key:
            raise ValueError("--manual-point-block must include DEVICE_ID:POINT before '='")
        device_id, point_key = key.split(":", 1)
        if not device_id.strip() or not point_key.strip() or not reason:
            raise ValueError("--manual-point-block requires device ID, point key, and reason")
        blocks[f"{device_id.strip()}:{point_key.strip()}"] = reason
    return blocks


def _load_component_snapshot(path: str | None) -> dict[str, object]:
    if not path:
        return {}
    snapshot_path = Path(path)
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        cfg = H2OSenco24CandidateConfig(
            min_points=int(args.min_points),
            min_wet_points=int(args.min_wet_points),
            max_condition_number=float(args.max_condition_number),
            fit_max_abs_error_mmol=float(args.fit_max_abs_error_mmol),
            design_max_relative_error_pct=float(args.design_max_relative_error_pct),
            relative_error_min_reference_mmol=float(args.relative_error_min_reference_mmol),
            exclude_device_ids=tuple(args.exclude_device_id or ()),
            manual_device_block_reasons=_parse_manual_blocks(args.manual_device_block or ()),
            manual_point_block_reasons=_parse_manual_point_blocks(args.manual_point_block or ()),
            component_snapshot=_load_component_snapshot(args.component_snapshot_json),
            require_component_snapshot_for_layer_review=bool(args.require_component_snapshot_for_layer_review),
            postwrite_verified_device_ids=tuple(args.postwrite_verified_device_id or ()),
            postwrite_verification_artifacts=tuple(args.postwrite_verification_artifact or ()),
            additional_h2o_roots=tuple(args.additional_h2o_run_dir or ()),
            dry_anchor_roots=tuple(args.dry_anchor_run_dir or ()),
            dry_anchor_min_temp_c=args.dry_anchor_min_temp_c,
            dry_anchor_max_temp_c=args.dry_anchor_max_temp_c,
            allow_pressure_qc_failed_device_ids=tuple(args.allow_pressure_qc_failed_device_id or ()),
            fit_temperature_source=str(args.fit_temperature_source or "analyzer_chamber"),
            fit_objective=str(args.fit_objective or "absolute_mmol"),
            factory_signal_health_summary_csv=args.factory_signal_health_summary_csv,
            state_transfer_summary_csv=args.state_transfer_summary_csv,
            state_transfer_max_raw_excess_shift_mmol=float(
                args.state_transfer_max_raw_excess_shift_mmol
            ),
            state_transfer_max_post_s6_relative_error_pct=float(
                args.state_transfer_max_post_s6_relative_error_pct
            ),
        )
        outputs = write_h2o_senco24_candidate_report(
            run_dir=args.run_dir,
            output_dir=args.output_dir,
            cfg=cfg,
            database_sidecar_json=args.database_sidecar_json,
        )
    except Exception as exc:
        print(f"V1.5 H2O SENCO2/SENCO4 candidate review export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
