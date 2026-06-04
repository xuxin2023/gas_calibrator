"""Write a dry-run V1.5 full calibration chain plan.

The command deliberately does not execute the planned stages. It writes a
reviewable sequence that stitches together the validated V1.5 pressure,
temperature, CO2, H2O, coefficient, evidence, database, and report tools.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from ..v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    run_supervised_full_flow,
    write_full_flow_plan,
    write_full_flow_state,
    write_full_flow_supervised_run,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a V1.5 full calibration dry-run chain plan.")
    parser.add_argument("--config", required=True, help="V1.5 runtime config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for the flow plan.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--operator", default="")
    parser.add_argument("--analyzer-id", default="multi_device")
    parser.add_argument("--pressure-reference-json", default=None)
    parser.add_argument("--standard-gases-json", default=None)
    parser.add_argument("--co2-queue-csv", default=None)
    parser.add_argument("--h2o-queue-csv", default=None)
    parser.add_argument("--temperature-h2o-points-parent", default=None)
    parser.add_argument("--reviewed-run-dir", default=None)
    parser.add_argument("--evidence-bundle-json", default=None)
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument(
        "--completed-step",
        action="append",
        default=[],
        help="Mark a prior stage as completed when regenerating resumable state.",
    )
    parser.add_argument(
        "--failed-step",
        action="append",
        default=[],
        help="Mark a prior stage as failed when regenerating resumable state.",
    )
    parser.add_argument("--allow-real-com", action="store_true", help="Unblock read-only/route real-COM stages in state only.")
    parser.add_argument("--allow-pressure-control", action="store_true", help="Unblock pressure-control stages in state only.")
    parser.add_argument("--allow-route-control", action="store_true", help="Unblock gas/water route stages in state only.")
    parser.add_argument(
        "--allow-writes",
        action="store_true",
        help="Record external write authorization in state; this planner still does not execute writes.",
    )
    parser.add_argument(
        "--supervised-run-ready-offline",
        action="store_true",
        help="Advance ready offline stages under the V1.5 supervisor. Physical stages remain blocked.",
    )
    parser.add_argument(
        "--execute-offline-commands",
        action="store_true",
        help="Actually run supervised offline commands. Without this flag, the supervisor only writes a planned-only event.",
    )
    parser.add_argument("--max-offline-steps", type=int, default=1)
    parser.add_argument(
        "--allow-database-import",
        action="store_true",
        help="Permit supervised execution of offline database import stages.",
    )
    parser.add_argument("--cwd", default=None, help="Working directory for supervised offline commands.")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    plan = build_full_flow_plan(
        config_path=args.config,
        output_dir=args.output_dir,
        run_id=args.run_id,
        operator=args.operator,
        analyzer_id=args.analyzer_id,
        pressure_reference_json=args.pressure_reference_json,
        standard_gases_json=args.standard_gases_json,
        co2_queue_csv=args.co2_queue_csv,
        h2o_queue_csv=args.h2o_queue_csv,
        temperature_h2o_points_parent=args.temperature_h2o_points_parent,
        reviewed_run_dir=args.reviewed_run_dir,
        evidence_bundle_json=args.evidence_bundle_json,
        reviewer=args.reviewer,
        approver=args.approver,
    )
    outputs = write_full_flow_plan(plan, args.output_dir)
    state = build_full_flow_state(
        plan,
        completed_steps=args.completed_step,
        failed_steps=args.failed_step,
        allow_real_com=bool(args.allow_real_com),
        allow_pressure_control=bool(args.allow_pressure_control),
        allow_route_control=bool(args.allow_route_control),
        allow_writes=bool(args.allow_writes),
    )
    outputs.update(write_full_flow_state(state, args.output_dir))
    if args.supervised_run_ready_offline:
        supervised = run_supervised_full_flow(
            plan,
            completed_steps=args.completed_step,
            failed_steps=args.failed_step,
            allow_real_com=bool(args.allow_real_com),
            allow_pressure_control=bool(args.allow_pressure_control),
            allow_route_control=bool(args.allow_route_control),
            allow_writes=bool(args.allow_writes),
            allow_database_import=bool(args.allow_database_import),
            execute_commands=bool(args.execute_offline_commands),
            max_steps=int(args.max_offline_steps),
            output_dir=args.output_dir,
            cwd=args.cwd,
        )
        outputs.update(write_full_flow_supervised_run(supervised, args.output_dir))
        outputs.update(write_full_flow_state(supervised.final_state, args.output_dir))
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
