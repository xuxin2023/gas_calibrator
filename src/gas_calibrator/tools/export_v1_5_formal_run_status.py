"""Export an offline V1.5 formal run status rollup."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..validation.v1_5_formal_run_status import (
    build_v1_5_formal_run_status,
    write_v1_5_formal_run_status_outputs,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a V1.5 formal run status dashboard from existing sidecars only."
    )
    parser.add_argument("--run-dir", required=True, help="Existing V1.5 run/evidence directory.")
    parser.add_argument("--output-dir", required=True, help="Directory for status JSON/Markdown/CSV outputs.")
    parser.add_argument("--initialization-readiness-json", default="", help="Optional explicit readiness JSON.")
    parser.add_argument(
        "--formal-initialization-controlled-executor-design-json",
        default="",
        help="Optional explicit controlled initialization executor design JSON.",
    )
    parser.add_argument(
        "--formal-initialization-readonly-com-preflight-design-json",
        default="",
        help="Optional explicit read-only initialization COM preflight design JSON.",
    )
    parser.add_argument(
        "--formal-initialization-readonly-com-preflight-blocked-executor-json",
        default="",
        help="Optional explicit blocked read-only initialization COM preflight executor JSON.",
    )
    parser.add_argument(
        "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
        default="",
        help="Optional explicit controlled read-only initialization COM preflight executor design JSON.",
    )
    parser.add_argument(
        "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
        default="",
        help="Optional explicit controlled blocked read-only initialization COM preflight executor JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-execution-contract-json",
        default="",
        help="Optional explicit read-only COM execution packet contract JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-execution-blocked-executor-json",
        default="",
        help="Optional explicit blocked read-only COM execution executor JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-execution-packet-validator-json",
        default="",
        help="Optional explicit read-only COM execution packet validator JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-execution-plan-preview-json",
        default="",
        help="Optional explicit read-only COM execution plan preview JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-minimal-executor-review-json",
        default="",
        help="Optional explicit read-only COM minimal executor review JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-minimal-executor-stub-json",
        default="",
        help="Optional explicit read-only COM minimal executor stub JSON.",
    )
    parser.add_argument(
        "--formal-readonly-com-minimal-executor-json",
        default="",
        help="Optional explicit minimal read-only COM executor JSON.",
    )
    parser.add_argument(
        "--route-physical-recovery-readiness-json",
        default="",
        help="Optional explicit route physical recovery readiness JSON.",
    )
    parser.add_argument(
        "--mature-route-continuity-gate-json",
        default="",
        help="Optional explicit mature route continuity gate JSON.",
    )
    parser.add_argument(
        "--pressure-s9-readiness-index-json",
        default="",
        help="Optional explicit pressure/SENCO9 readiness index JSON.",
    )
    parser.add_argument("--pre-gas-readiness-json", default="", help="Optional explicit pre-gas readiness JSON.")
    parser.add_argument(
        "--batch-initialization-closeout-json",
        default="",
        help="Optional explicit batch initialization closeout index JSON.",
    )
    parser.add_argument(
        "--post-closeout-resume-gate-json",
        default="",
        help="Optional explicit post-closeout resume gate JSON.",
    )
    parser.add_argument(
        "--resume-prefix-application-review-json",
        default="",
        help="Optional explicit resume-prefix state-application review JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-state-writer-design-json",
        default="",
        help="Optional explicit authoritative resume-state writer design JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-state-writer-blocked-executor-json",
        default="",
        help="Optional explicit authoritative resume-state writer blocked executor JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-state-controlled-write-preflight-json",
        default="",
        help="Optional explicit authoritative resume-state controlled-write preflight JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-state-atomic-write-json",
        default="",
        help="Optional explicit authoritative resume-state atomic-write evidence JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-state-post-write-verification-json",
        default="",
        help="Optional explicit authoritative resume-state post-write verification JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-offline-state-advance-atomic-write-json",
        default="",
        help="Optional explicit manual offline state-advance atomic writer JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-offline-state-advance-post-write-verification-json",
        default="",
        help="Optional explicit offline state-advance post-write verification JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-offline-state-advance-consumer-readiness-json",
        default="",
        help="Optional explicit offline-advanced resume-state consumer readiness JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-offline-state-advance-next-step-plan-json",
        default="",
        help="Optional explicit offline-advanced resume-state next-step plan JSON.",
    )
    parser.add_argument(
        "--authoritative-resume-offline-state-advance-next-step-authorization-preflight-json",
        default="",
        help="Optional explicit offline next-step review authorization preflight JSON.",
    )
    parser.add_argument("--getco-readiness-json", default="", help="Optional explicit identity/GETCO readiness JSON.")
    parser.add_argument("--run-evidence-status-json", default="", help="Optional explicit run evidence status JSON.")
    parser.add_argument(
        "--full-flow-closure-readiness-json",
        default="",
        help="Optional explicit full-flow closure readiness JSON.",
    )
    parser.add_argument("--archive-closure-json", default="", help="Optional explicit formal archive closure JSON.")
    parser.add_argument(
        "--algorithm-profile-runner-dry-run-json",
        default="",
        help="Optional explicit new-algorithm profile runner dry-run bundle JSON.",
    )
    parser.add_argument(
        "--full-flow-automation-closure-json",
        default="",
        help="Optional explicit V1.5 full-flow automation closure map JSON.",
    )
    parser.add_argument(
        "--senco-artifact-authorization-json",
        default="",
        help="Optional explicit main SENCO artifact authorization JSON.",
    )
    parser.add_argument(
        "--formal-database-dry-run-json",
        default="",
        help="Optional explicit PostgreSQL 18 formal database dry-run contract JSON.",
    )
    parser.add_argument(
        "--formal-database-import-preflight-json",
        default="",
        help="Optional explicit PostgreSQL 18 formal database import preflight JSON.",
    )
    parser.add_argument(
        "--formal-database-import-authorization-json",
        default="",
        help="Optional explicit PostgreSQL 18 formal database import authorization JSON.",
    )
    parser.add_argument(
        "--formal-database-import-command-contract-json",
        default="",
        help="Optional explicit PostgreSQL 18 formal database import command contract JSON.",
    )
    parser.add_argument(
        "--formal-database-import-blocked-executor-json",
        default="",
        help="Optional explicit PostgreSQL 18 blocked import executor JSON.",
    )
    parser.add_argument(
        "--formal-database-import-controlled-executor-design-json",
        default="",
        help="Optional explicit PostgreSQL 18 controlled import executor design JSON.",
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Return exit code 2 when the rollup is blocked.",
    )
    parser.add_argument(
        "--fail-on-not-release-ready",
        action="store_true",
        help="Return exit code 3 unless formal_release_allowed is true.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        model = build_v1_5_formal_run_status(
            run_dir=args.run_dir,
            initialization_readiness_json=args.initialization_readiness_json or None,
            formal_initialization_controlled_executor_design_json=(
                args.formal_initialization_controlled_executor_design_json or None
            ),
            formal_initialization_readonly_com_preflight_design_json=(
                args.formal_initialization_readonly_com_preflight_design_json or None
            ),
            formal_initialization_readonly_com_preflight_blocked_executor_json=(
                args.formal_initialization_readonly_com_preflight_blocked_executor_json or None
            ),
            formal_initialization_readonly_com_preflight_controlled_executor_design_json=(
                args.formal_initialization_readonly_com_preflight_controlled_executor_design_json or None
            ),
            formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=(
                args.formal_initialization_readonly_com_preflight_controlled_blocked_executor_json or None
            ),
            formal_readonly_com_execution_contract_json=args.formal_readonly_com_execution_contract_json or None,
            formal_readonly_com_execution_blocked_executor_json=(
                args.formal_readonly_com_execution_blocked_executor_json or None
            ),
            formal_readonly_com_execution_packet_validator_json=(
                args.formal_readonly_com_execution_packet_validator_json or None
            ),
            formal_readonly_com_execution_plan_preview_json=(
                args.formal_readonly_com_execution_plan_preview_json or None
            ),
            formal_readonly_com_minimal_executor_review_json=(
                args.formal_readonly_com_minimal_executor_review_json or None
            ),
            formal_readonly_com_minimal_executor_stub_json=(
                args.formal_readonly_com_minimal_executor_stub_json or None
            ),
            formal_readonly_com_minimal_executor_json=(
                args.formal_readonly_com_minimal_executor_json or None
            ),
            route_physical_recovery_readiness_json=(
                args.route_physical_recovery_readiness_json or None
            ),
            mature_route_continuity_gate_json=args.mature_route_continuity_gate_json or None,
            pressure_s9_readiness_index_json=args.pressure_s9_readiness_index_json or None,
            pre_gas_readiness_json=args.pre_gas_readiness_json or None,
            batch_initialization_closeout_json=args.batch_initialization_closeout_json or None,
            post_closeout_resume_gate_json=args.post_closeout_resume_gate_json or None,
            resume_prefix_application_review_json=(
                args.resume_prefix_application_review_json or None
            ),
            authoritative_resume_state_writer_design_json=(
                args.authoritative_resume_state_writer_design_json or None
            ),
            authoritative_resume_state_writer_blocked_executor_json=(
                args.authoritative_resume_state_writer_blocked_executor_json or None
            ),
            authoritative_resume_state_controlled_write_preflight_json=(
                args.authoritative_resume_state_controlled_write_preflight_json or None
            ),
            authoritative_resume_state_atomic_write_json=(
                args.authoritative_resume_state_atomic_write_json or None
            ),
            authoritative_resume_state_post_write_verification_json=(
                args.authoritative_resume_state_post_write_verification_json or None
            ),
            authoritative_resume_offline_state_advance_atomic_write_json=(
                args.authoritative_resume_offline_state_advance_atomic_write_json or None
            ),
            authoritative_resume_offline_state_advance_post_write_verification_json=(
                args.authoritative_resume_offline_state_advance_post_write_verification_json
                or None
            ),
            authoritative_resume_offline_state_advance_consumer_readiness_json=(
                args.authoritative_resume_offline_state_advance_consumer_readiness_json
                or None
            ),
            authoritative_resume_offline_state_advance_next_step_plan_json=(
                args.authoritative_resume_offline_state_advance_next_step_plan_json
                or None
            ),
            authoritative_resume_offline_state_advance_next_step_authorization_preflight_json=(
                args.authoritative_resume_offline_state_advance_next_step_authorization_preflight_json
                or None
            ),
            getco_readiness_json=args.getco_readiness_json or None,
            run_evidence_status_json=args.run_evidence_status_json or None,
            full_flow_closure_readiness_json=args.full_flow_closure_readiness_json or None,
            archive_closure_json=args.archive_closure_json or None,
            algorithm_profile_runner_dry_run_json=args.algorithm_profile_runner_dry_run_json or None,
            full_flow_automation_closure_json=args.full_flow_automation_closure_json or None,
            senco_artifact_authorization_json=args.senco_artifact_authorization_json or None,
            formal_database_dry_run_json=args.formal_database_dry_run_json or None,
            formal_database_import_preflight_json=args.formal_database_import_preflight_json or None,
            formal_database_import_authorization_json=args.formal_database_import_authorization_json or None,
            formal_database_import_command_contract_json=args.formal_database_import_command_contract_json or None,
            formal_database_import_blocked_executor_json=args.formal_database_import_blocked_executor_json or None,
            formal_database_import_controlled_executor_design_json=(
                args.formal_database_import_controlled_executor_design_json or None
            ),
        )
        outputs = write_v1_5_formal_run_status_outputs(model, Path(args.output_dir))
        result = {
            "overall_status": model.get("overall_status"),
            "current_stage": model.get("current_stage"),
            "next_action": model.get("next_action"),
            "formal_release_allowed": model.get("formal_release_allowed"),
            "database_import_allowed": model.get("database_import_allowed"),
            "can_continue_physical_flow": model.get("can_continue_physical_flow"),
            "outputs": outputs,
            "physical_boundaries": model.get("physical_boundaries"),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if args.fail_on_blocked and model.get("overall_status") == "blocked":
            return 2
        if args.fail_on_not_release_ready and not model.get("formal_release_allowed"):
            return 3
        return 0
    except Exception as exc:
        print(f"V1.5 formal run status export failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
