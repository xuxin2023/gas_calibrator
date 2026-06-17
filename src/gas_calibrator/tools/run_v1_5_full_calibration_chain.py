"""Write a dry-run V1.5 full calibration chain plan.

The command deliberately does not execute the planned stages. It writes a
reviewable sequence that stitches together the validated V1.5 pressure,
temperature, CO2, H2O, coefficient, evidence, database, and report tools.
"""

from __future__ import annotations

import argparse
import json
import sys
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
from ..validation.v1_5_formal_flow_contract import (
    read_json,
    render_v1_5_formal_flow_contract_markdown,
    validate_v1_5_formal_flow_contract,
)
from ..validation.v1_5_formal_archive_closure import build_v1_5_formal_archive_closure
from ..validation.v1_5_full_flow_closure_readiness import (
    build_v1_5_full_flow_closure_readiness,
    write_v1_5_full_flow_closure_readiness_outputs,
)
from ..validation.v1_5_post_run_coefficient_executor import (
    build_post_run_coefficient_executor_model,
    write_post_run_coefficient_executor_outputs,
)
from ..validation.v1_5_run_evidence_status import (
    build_v1_5_run_evidence_status,
    render_v1_5_run_evidence_status_markdown,
)


def _write_run_evidence_status(
    *,
    run_dir: str | Path,
    output_dir: str | Path,
    full_flow_plan_json: str | Path | None,
    contract_json: str | Path | None,
    evidence_bundle_json: str | Path | None,
) -> tuple[dict, Path, Path]:
    """Refresh the offline evidence index without touching devices."""

    evidence_status = build_v1_5_run_evidence_status(
        run_dir=run_dir,
        full_flow_plan_json=full_flow_plan_json,
        contract_json=contract_json,
        evidence_bundle_json=evidence_bundle_json,
    )
    status_json = Path(output_dir).resolve() / "v1_5_run_evidence_status.json"
    status_md = Path(output_dir).resolve() / "v1_5_run_evidence_status.md"
    status_json.write_text(json.dumps(evidence_status, ensure_ascii=False, indent=2), encoding="utf-8")
    status_md.write_text(render_v1_5_run_evidence_status_markdown(evidence_status), encoding="utf-8")
    return evidence_status, status_json, status_md


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
    parser.add_argument("--inventory-json", default=None, help="Optional V1.5 entrypoint inventory JSON for contract audit.")
    parser.add_argument(
        "--archive-closure",
        action="store_true",
        help="After status generation, build the offline formal archive closure inside reviewed-run-dir.",
    )
    parser.add_argument(
        "--archive-plan-json",
        default=None,
        help="Formal calibration plan snapshot JSON used by archive closure.",
    )
    parser.add_argument(
        "--archive-pressure-reference-json",
        default=None,
        help="COM22 pressure-reference snapshot JSON for archive closure. Defaults to --pressure-reference-json.",
    )
    parser.add_argument(
        "--archive-standard-gases-json",
        default=None,
        help="Reviewed standard-gases JSON for archive closure. Defaults to --standard-gases-json.",
    )
    parser.add_argument(
        "--archive-output-dir",
        default=None,
        help="Archive closure output directory. Must be inside reviewed-run-dir.",
    )
    parser.add_argument("--archive-pressure-check-csv", default=None)
    parser.add_argument("--archive-component", choices=("co2", "h2o", "both"), default="both")
    parser.add_argument("--archive-analyzer-prefix", default="all")
    parser.add_argument("--archive-today", default=None)
    parser.add_argument("--archive-allow-pressure-fallback", action="store_true")
    parser.add_argument("--archive-report-no", default="")
    parser.add_argument("--archive-location", default="")
    parser.add_argument("--archive-calibration-date", default="")
    parser.add_argument("--archive-uncertainty-json", default=None)
    parser.add_argument("--archive-db-mode", choices=("skip", "dry-run", "dry_run", "import"), default="dry-run")
    parser.add_argument("--archive-dsn", default=None)
    parser.add_argument("--archive-apply-migrations", action="store_true")
    parser.add_argument(
        "--post-run-coefficient-executor",
        action="store_true",
        help="Build the offline no-write post-run coefficient closure plan after evidence status/archive refresh.",
    )
    parser.add_argument(
        "--post-run-executor-output-dir",
        default=None,
        help="Output directory for the post-run coefficient executor. Defaults to output-dir/post_run_coefficient_executor.",
    )
    parser.add_argument(
        "--post-run-executor-fail-on-blocked",
        action="store_true",
        help="Return non-zero if the post-run coefficient executor status is blocked.",
    )
    parser.add_argument(
        "--full-flow-closure-readiness",
        action="store_true",
        help=(
            "Build the offline full-flow closure readiness review after acquisition. "
            "This automatically generates the no-write post-run coefficient executor "
            "first when it was not requested explicitly."
        ),
    )
    parser.add_argument(
        "--full-flow-closure-readiness-output-dir",
        default=None,
        help="Output directory for closure readiness. Defaults to output-dir/full_flow_closure_readiness.",
    )
    parser.add_argument(
        "--full-flow-closure-readiness-fail-on-blocked",
        action="store_true",
        help="Return non-zero if the full-flow closure readiness status is blocked.",
    )
    parser.add_argument(
        "--skip-contract-audit",
        action="store_true",
        help="Skip writing the V1.5 formal flow contract audit.",
    )
    parser.add_argument(
        "--fail-on-contract-blocked",
        action="store_true",
        help="Return non-zero when the generated contract audit is blocked.",
    )
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
    contract_status = "skipped"
    if not args.skip_contract_audit:
        inventory = read_json(args.inventory_json) if args.inventory_json else None
        contract = validate_v1_5_formal_flow_contract(plan, inventory_entries=inventory)
        contract_json = Path(args.output_dir).resolve() / "v1_5_formal_flow_contract.json"
        contract_md = Path(args.output_dir).resolve() / "v1_5_formal_flow_contract.md"
        contract_json.write_text(json.dumps(contract.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
        contract_md.write_text(render_v1_5_formal_flow_contract_markdown(contract), encoding="utf-8")
        outputs["contract_json"] = contract_json
        outputs["contract_markdown"] = contract_md
        contract_status = contract.status
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
    status_run_dir = Path(args.reviewed_run_dir).resolve() if args.reviewed_run_dir else Path(args.output_dir).resolve()
    evidence_status, status_json, status_md = _write_run_evidence_status(
        run_dir=status_run_dir,
        output_dir=args.output_dir,
        full_flow_plan_json=outputs.get("plan_json"),
        contract_json=outputs.get("contract_json"),
        evidence_bundle_json=args.evidence_bundle_json,
    )
    outputs["run_evidence_status_json"] = status_json
    outputs["run_evidence_status_markdown"] = status_md
    latest_evidence_bundle_json = args.evidence_bundle_json
    if args.archive_closure:
        pressure_reference_json = args.archive_pressure_reference_json or args.pressure_reference_json
        archive_standard_gases_json = args.archive_standard_gases_json or args.standard_gases_json
        missing = []
        if not args.reviewed_run_dir:
            missing.append("--reviewed-run-dir")
        if not args.archive_plan_json:
            missing.append("--archive-plan-json")
        if not pressure_reference_json:
            missing.append("--archive-pressure-reference-json or --pressure-reference-json")
        if missing:
            print(
                "Archive closure requires " + ", ".join(missing),
                file=sys.stderr,
                flush=True,
            )
            return 2
        archive_output_dir = (
            Path(args.archive_output_dir).resolve()
            if args.archive_output_dir
            else Path(status_run_dir).resolve() / "formal_archive_closure_from_full_chain"
        )
        archive = build_v1_5_formal_archive_closure(
            run_dir=status_run_dir,
            plan_json=args.archive_plan_json,
            pressure_reference_json=pressure_reference_json,
            standard_gases_json=archive_standard_gases_json,
            output_dir=archive_output_dir,
            pressure_check_csv=args.archive_pressure_check_csv,
            component=args.archive_component,
            analyzer_prefix=args.archive_analyzer_prefix,
            today=args.archive_today,
            allow_pressure_fallback=bool(args.archive_allow_pressure_fallback),
            report_no=args.archive_report_no or str(args.run_id or ""),
            reviewer=args.reviewer,
            approver=args.approver,
            location=args.archive_location,
            calibration_date=args.archive_calibration_date,
            uncertainty_json=args.archive_uncertainty_json,
            db_mode=args.archive_db_mode,
            dsn=args.archive_dsn,
            apply_db_migrations=bool(args.archive_apply_migrations),
        )
        archive_paths = archive["paths"]
        outputs["archive_closure_index_json"] = archive_paths["archive_index_json"]
        outputs["archive_closure_index_markdown"] = archive_paths["archive_index_markdown"]
        outputs["archive_closure_evidence_bundle"] = archive_paths["evidence_bundle"]
        outputs["archive_closure_traceability_summary"] = archive_paths["traceability_summary"]
        outputs["archive_closure_database_summary"] = archive_paths["database_import_summary"]
        latest_evidence_bundle_json = archive_paths["evidence_bundle"]
        evidence_status, _, _ = _write_run_evidence_status(
            run_dir=status_run_dir,
            output_dir=args.output_dir,
            full_flow_plan_json=outputs.get("plan_json"),
            contract_json=outputs.get("contract_json"),
            evidence_bundle_json=latest_evidence_bundle_json,
        )
        outputs["run_evidence_status_refreshed_after_archive_closure"] = status_json
        outputs["run_evidence_status_evidence_bundle_json"] = latest_evidence_bundle_json
    executor_model = None
    should_build_post_run_executor = bool(args.post_run_coefficient_executor or args.full_flow_closure_readiness)
    if should_build_post_run_executor:
        executor_run_dir = Path(args.reviewed_run_dir).resolve() if args.reviewed_run_dir else Path(args.output_dir).resolve()
        executor_output_dir = (
            Path(args.post_run_executor_output_dir).resolve()
            if args.post_run_executor_output_dir
            else Path(args.output_dir).resolve() / "post_run_coefficient_executor"
        )
        executor_model = build_post_run_coefficient_executor_model(
            run_dir=executor_run_dir,
            plan_json=args.archive_plan_json or outputs.get("plan_json"),
            pressure_reference_json=args.archive_pressure_reference_json
            or args.pressure_reference_json
            or None,
            run_evidence_status_json=status_json,
            archive_closure_json=outputs.get("archive_closure_index_json"),
        )
        executor_paths = write_post_run_coefficient_executor_outputs(executor_model, executor_output_dir)
        outputs["post_run_coefficient_executor_manifest"] = executor_paths["manifest"]
        outputs["post_run_coefficient_executor_summary"] = executor_paths["summary"]
        outputs["post_run_coefficient_executor_stages"] = executor_paths["stages"]
        outputs["post_run_coefficient_executor_devices"] = executor_paths["devices"]
        outputs["post_run_coefficient_executor_execution_plan"] = executor_paths["execution_plan"]
        outputs["post_run_coefficient_executor_controlled_write_package"] = executor_paths["controlled_write_package"]
        outputs["post_run_coefficient_executor_post_write_reverification_plan"] = executor_paths[
            "post_write_reverification_plan"
        ]
        outputs["post_run_coefficient_executor_archive_gap_list"] = executor_paths["archive_gap_list"]
    closure_model = None
    if args.full_flow_closure_readiness:
        closure_output_dir = (
            Path(args.full_flow_closure_readiness_output_dir).resolve()
            if args.full_flow_closure_readiness_output_dir
            else Path(args.output_dir).resolve() / "full_flow_closure_readiness"
        )
        closure_model = build_v1_5_full_flow_closure_readiness(
            run_dir=Path(args.output_dir).resolve(),
            full_flow_plan_json=outputs.get("plan_json"),
            run_evidence_status_json=status_json,
            post_run_executor_json=outputs.get("post_run_coefficient_executor_manifest"),
            archive_closure_json=outputs.get("archive_closure_index_json"),
            controlled_write_package_csv=outputs.get("post_run_coefficient_executor_controlled_write_package"),
            post_write_reverification_plan_csv=outputs.get(
                "post_run_coefficient_executor_post_write_reverification_plan"
            ),
            archive_gap_list_csv=outputs.get("post_run_coefficient_executor_archive_gap_list"),
        )
        closure_paths = write_v1_5_full_flow_closure_readiness_outputs(closure_model, closure_output_dir)
        outputs["full_flow_closure_readiness_json"] = closure_paths["readiness_json"]
        outputs["full_flow_closure_readiness_markdown"] = closure_paths["readiness_markdown"]
        outputs["full_flow_closure_readiness_stages"] = closure_paths["stages"]
        outputs["full_flow_closure_readiness_gaps"] = closure_paths["gaps"]
        outputs["full_flow_closure_readiness_devices"] = closure_paths["devices"]
        outputs["full_flow_closure_readiness_release_domains"] = closure_paths["release_domains"]
    if args.archive_closure or should_build_post_run_executor or args.full_flow_closure_readiness:
        evidence_status, _, _ = _write_run_evidence_status(
            run_dir=Path(args.output_dir).resolve(),
            output_dir=args.output_dir,
            full_flow_plan_json=outputs.get("plan_json"),
            contract_json=outputs.get("contract_json"),
            evidence_bundle_json=latest_evidence_bundle_json,
        )
        outputs["run_evidence_status_final_json"] = status_json
        outputs["run_evidence_status_final_markdown"] = status_md
    print(json.dumps({key: str(Path(value).resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    if args.fail_on_contract_blocked and contract_status == "blocked":
        return 2
    if should_build_post_run_executor and args.post_run_executor_fail_on_blocked:
        if executor_model.get("overall_status") == "blocked":
            return 2
    if args.full_flow_closure_readiness and args.full_flow_closure_readiness_fail_on_blocked:
        if closure_model and closure_model.get("overall_status") == "blocked":
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
