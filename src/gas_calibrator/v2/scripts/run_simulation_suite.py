from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from ..core.offline_artifacts import build_suite_case_metadata, export_suite_offline_artifacts
from ..ui_v2.i18n import (
    display_compare_status,
    display_evidence_source,
    display_evidence_state,
    display_phase,
    display_risk_level,
    display_suite_failure_type,
    t,
)
from ..sim import (
    DEFAULT_REPLAY_FIXTURE_ROOT,
    build_export_resilience_report,
    build_summary_parity_report,
    get_simulation_suite,
    list_replay_scenarios,
    list_simulation_suites,
    load_replay_fixture,
    materialize_replay_fixture,
)
from .run_simulated_compare import build_simulated_compare_result


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=t("suite.cli_description"))
    parser.add_argument("--suite", default="smoke", help=t("suite.arg.suite"))
    parser.add_argument(
        "--report-root",
        default=str(Path(__file__).resolve().parents[2] / "output" / "v1_v2_compare"),
        help=t("suite.arg.report_root"),
    )
    parser.add_argument("--run-name", default=None, help=t("suite.arg.run_name"))
    parser.add_argument("--list-suites", action="store_true", help=t("suite.arg.list_suites"))
    return parser.parse_args(list(argv) if argv is not None else None)


def run_suite(*, suite_name: str, report_root: Path, run_name: Optional[str] = None) -> dict[str, object]:
    suite = get_simulation_suite(suite_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_dir = report_root / str(run_name or f"suite_{suite.name}_{timestamp}")
    suite_dir.mkdir(parents=True, exist_ok=True)
    case_results: list[dict[str, object]] = []
    passed = 0
    failed = 0

    for case in suite.cases:
        if case.kind == "scenario":
            result = build_simulated_compare_result(
                profile=str(case.profile or ""),
                scenario=str(case.scenario or case.name),
                fixture=None,
                report_root=suite_dir,
                run_name=case.name,
                publish_latest=False,
            )
            status = str(result.get("compare_status") or "")
            artifact_dir = str(result.get("report_dir") or "")
            details = {
                "report_json": result.get("report_json"),
                "report_markdown": result.get("report_markdown"),
            }
        elif case.kind == "replay":
            payload = load_replay_fixture(scenario=case.name, root=DEFAULT_REPLAY_FIXTURE_ROOT)
            result = materialize_replay_fixture(
                payload,
                report_root=suite_dir,
                run_name=case.name,
                publish_latest=False,
                evidence_state_override="replay",
            )
            status = str(result.get("compare_status") or "")
            artifact_dir = str(result.get("report_dir") or "")
            details = {
                "report_json": result.get("report_json"),
                "report_markdown": result.get("report_markdown"),
            }
        elif case.kind == "parity":
            result = build_summary_parity_report(report_root=suite_dir, run_name=case.name)
            status = str(result.get("status") or "")
            artifact_dir = str(result.get("report_dir") or "")
            report_payload = dict(result.get("report") or {})
            details = {
                "report_json": result.get("report_json"),
                "report_markdown": result.get("report_markdown"),
                "comparison_summary": dict(report_payload.get("summary", {}) or {}),
                "tolerance_rules": dict(report_payload.get("tolerance_rules", {}) or {}),
                "expected_divergence": list(report_payload.get("expected_divergence", []) or []),
            }
        elif case.kind == "resilience":
            result = build_export_resilience_report(report_root=suite_dir, run_name=case.name)
            status = str(result.get("status") or "")
            artifact_dir = str(result.get("report_dir") or "")
            report_payload = dict(result.get("report") or {})
            details = {
                "report_json": result.get("report_json"),
                "report_markdown": result.get("report_markdown"),
                "case_statuses": {
                    str(item.get("name") or ""): str(item.get("status") or "")
                    for item in list(report_payload.get("cases", []) or [])
                },
            }
        else:
            status = "UNKNOWN_CASE_KIND"
            artifact_dir = ""
            details = {"error": f"unsupported suite case kind: {case.kind}"}
        ok = status in set(case.expected_statuses)
        case_metadata = build_suite_case_metadata(
            {
                "name": case.name,
                "kind": case.kind,
                "status": status,
                "ok": ok,
                "artifact_dir": artifact_dir,
                "details": details,
            },
            suite_name=suite.name,
        )
        passed += 1 if ok else 0
        failed += 0 if ok else 1
        case_results.append(
            {
                "name": case.name,
                "kind": case.kind,
                "expected_statuses": list(case.expected_statuses),
                "status": status,
                "ok": ok,
                "artifact_dir": artifact_dir,
                "details": details,
                **case_metadata,
            }
        )

    summary = {
        "suite": suite.name,
        "description": suite.description,
        "report_dir": str(suite_dir),
        "failed_cases": [case["name"] for case in case_results if not bool(case["ok"])],
        "cases": case_results,
        "counts": {
            "total": len(case_results),
            "passed": passed,
            "failed": failed,
        },
        "all_passed": failed == 0,
    }
    summary_path = suite_dir / "suite_summary.json"
    summary_markdown_path = suite_dir / "suite_summary.md"
    offline_payload = export_suite_offline_artifacts(suite_dir=suite_dir, summary=summary)
    summary["summary_json"] = str(summary_path)
    summary["summary_markdown"] = str(summary_markdown_path)
    summary["suite_analytics_summary"] = str(suite_dir / "suite_analytics_summary.json")
    summary["suite_acceptance_plan"] = str(suite_dir / "suite_acceptance_plan.json")
    summary["suite_evidence_registry"] = str(suite_dir / "suite_evidence_registry.json")
    summary["suite_digest"] = dict(offline_payload.get("suite_analytics_summary", {}).get("digest") or {})
    summary["acceptance_readiness_summary"] = dict(
        offline_payload.get("suite_acceptance_plan", {}).get("readiness_summary") or {}
    )
    summary["role_views"] = dict(offline_payload.get("suite_acceptance_plan", {}).get("role_views") or {})
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary_markdown_path.write_text(_suite_summary_markdown(summary), encoding="utf-8")
    return summary


def _suite_summary_markdown(summary: dict[str, object]) -> str:
    counts = dict(summary.get("counts") or {})
    cases = list(summary.get("cases") or [])
    failed_cases = list(summary.get("failed_cases") or [])
    suite_digest = dict(summary.get("suite_digest") or {})
    readiness = dict(summary.get("acceptance_readiness_summary") or {})
    role_views = dict(summary.get("role_views") or {})
    failure_types: dict[str, int] = {}
    failure_phases: dict[str, int] = {}
    for case in cases:
        if bool(case.get("ok", False)):
            continue
        failure_type = str(case.get("failure_type") or "none")
        failure_phase = str(case.get("failure_phase") or "--")
        failure_types[failure_type] = failure_types.get(failure_type, 0) + 1
        failure_phases[failure_phase] = failure_phases.get(failure_phase, 0) + 1
    lines = [
        f"# {t('suite.summary_title')}",
        "",
        f"- {t('suite.suite')}: {summary.get('suite', '--')}",
        f"- {t('suite.description')}: {summary.get('description', '--')}",
        f"- {t('suite.report_dir')}: {summary.get('report_dir', '--')}",
        f"- {t('suite.total_cases')}: {counts.get('total', 0)}",
        f"- {t('suite.passed')}: {counts.get('passed', 0)}",
        f"- {t('suite.failed')}: {counts.get('failed', 0)}",
        f"- {t('suite.acceptance_readiness')}: {readiness.get('summary', '--')}",
        f"- {t('suite.analytics_digest')}: {suite_digest.get('summary', '--')}",
        f"- {t('suite.operator_view')}: {dict(role_views.get('operator') or {}).get('summary', '--')}",
        f"- {t('suite.reviewer_view')}: {dict(role_views.get('reviewer') or {}).get('summary', '--')}",
        f"- {t('suite.approver_view')}: {dict(role_views.get('approver') or {}).get('summary', '--')}",
        "",
        f"## {t('suite.evidence_artifacts')}",
        f"- {t('suite.suite_summary_json')}: {summary.get('summary_json', '--')}",
        f"- {t('suite.suite_analytics_summary')}: {summary.get('suite_analytics_summary', '--')}",
        f"- {t('suite.suite_acceptance_plan')}: {summary.get('suite_acceptance_plan', '--')}",
        f"- {t('suite.suite_evidence_registry')}: {summary.get('suite_evidence_registry', '--')}",
        "",
        f"## {t('suite.failed_cases')}",
    ]
    if failed_cases:
        lines.extend(f"- {name}" for name in failed_cases)
    else:
        lines.append(f"- {t('suite.none')}")
    lines.extend(["", f"## {t('suite.failure_review')}"])
    if failure_types:
        lines.extend(
            t(
                "suite.failure_type",
                name=display_suite_failure_type(name, default=str(name)),
                count=count,
            )
            for name, count in sorted(failure_types.items())
        )
        lines.extend(
            t(
                "suite.failure_phase",
                name=display_phase(name, default=str(name)),
                count=count,
            )
            for name, count in sorted(failure_phases.items())
        )
    else:
        lines.append(f"- {t('suite.no_failed_cases')}")
    lines.extend(["", f"## {t('suite.case_results')}"])
    for case in cases:
        lines.append(
            f"- {t('suite.case_line', name=case.get('name', '--'), status=display_compare_status(case.get('status', '--'), default=str(case.get('status', '--'))), expected=', '.join(display_compare_status(item, default=str(item)) for item in case.get('expected_statuses', []) or []), artifact_dir=case.get('artifact_dir', '--') or '--')}"
        )
        lines.append(
            f"  {t('suite.case_detail', evidence_source=display_evidence_source(case.get('evidence_source', '--'), default=str(case.get('evidence_source', '--'))), evidence_state=display_evidence_state(case.get('evidence_state', '--'), default=str(case.get('evidence_state', '--'))), risk_level=display_risk_level(case.get('risk_level', '--'), default=str(case.get('risk_level', '--'))), failure_type=display_suite_failure_type(case.get('failure_type', '--'), default=str(case.get('failure_type', '--'))), failure_phase=display_phase(case.get('failure_phase', '--'), default=str(case.get('failure_phase', '--'))))}"
        )
        details = dict(case.get("details") or {})
        lines.append(
            f"  {t('suite.review_artifacts', json_path=details.get('report_json', '--'), markdown_path=details.get('report_markdown', '--'))}"
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.list_suites:
        for item in list_simulation_suites():
            print(item)
        return 0
    summary = run_suite(
        suite_name=str(args.suite),
        report_root=Path(str(args.report_root)).resolve(),
        run_name=args.run_name,
    )
    print(t("suite.cli_suite", value=summary["suite"]))
    print(t("suite.cli_report_dir", value=summary["report_dir"]))
    print(t("suite.cli_passed", passed=summary["counts"]["passed"], total=summary["counts"]["total"]))
    print(t("suite.cli_summary_markdown", value=summary["summary_markdown"]))
    for case in summary["cases"]:
        print(
            "- "
            + t(
                "suite.cli_case",
                name=case["name"],
                status=display_compare_status(case["status"], default=str(case["status"])),
                expected=",".join(
                    display_compare_status(item, default=str(item)) for item in case["expected_statuses"]
                ),
            )
        )
    return 0 if bool(summary["all_passed"]) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
