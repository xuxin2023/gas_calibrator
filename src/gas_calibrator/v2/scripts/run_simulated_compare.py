from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..ui_v2.i18n import display_compare_status, display_evidence_source, t
from ..sim import (
    DEFAULT_REPLAY_FIXTURE_ROOT,
    build_protocol_simulated_compare_result,
    get_simulated_scenario,
    list_simulated_profiles,
    list_simulated_scenarios,
    load_replay_fixture,
    materialize_replay_fixture,
    simulated_profile_defaults,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=t("compare.cli_description"))
    parser.add_argument(
        "--profile",
        default="replacement_skip0_co2_only_simulated",
        help=t("compare.arg.profile"),
    )
    parser.add_argument("--scenario", default=None, help=t("compare.arg.scenario"))
    parser.add_argument("--fixture", default=None, help=t("compare.arg.fixture"))
    parser.add_argument(
        "--report-root",
        default=str(Path(__file__).resolve().parents[2] / "output" / "v1_v2_compare"),
        help=t("compare.arg.report_root"),
    )
    parser.add_argument("--run-name", default=None, help=t("compare.arg.run_name"))
    parser.add_argument(
        "--publish-latest",
        action="store_true",
        help=t("compare.arg.publish_latest"),
    )
    parser.add_argument(
        "--no-publish-latest",
        action="store_true",
        help=t("compare.arg.no_publish_latest"),
    )
    parser.add_argument("--list-scenarios", action="store_true", help=t("compare.arg.list_scenarios"))
    parser.add_argument("--list-profiles", action="store_true", help=t("compare.arg.list_profiles"))
    return parser.parse_args(list(argv) if argv is not None else None)


def build_simulated_compare_result(
    *,
    profile: str,
    scenario: Optional[str],
    fixture: Optional[str],
    report_root: Path,
    run_name: Optional[str],
    publish_latest: bool,
) -> dict[str, object]:
    defaults = simulated_profile_defaults(profile)
    scenario_name = str(scenario or defaults["scenario"])
    scenario_def = get_simulated_scenario(scenario_name)
    if str(scenario_def.execution_mode or "fixture").strip().lower() == "protocol" and fixture is None:
        return build_protocol_simulated_compare_result(
            profile=profile,
            scenario=scenario_name,
            report_root=report_root,
            run_name=run_name,
            publish_latest=publish_latest,
        )
    payload = load_replay_fixture(
        scenario=scenario_def.fixture_name if fixture is None else None,
        fixture=fixture,
        root=DEFAULT_REPLAY_FIXTURE_ROOT,
    )
    return materialize_replay_fixture(
        payload,
        report_root=report_root,
        run_name=run_name,
        publish_latest=publish_latest,
        validation_profile_override=profile,
        simulation_context_override=scenario_def.simulation_context(),
        evidence_state_override="simulated_fixture",
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.list_profiles:
        for item in list_simulated_profiles():
            print(item)
        return 0
    if args.list_scenarios:
        for item in list_simulated_scenarios():
            print(item)
        return 0
    publish_latest = not bool(args.no_publish_latest)
    result = build_simulated_compare_result(
        profile=str(args.profile),
        scenario=args.scenario,
        fixture=args.fixture,
        report_root=Path(str(args.report_root)).resolve(),
        run_name=args.run_name,
        publish_latest=publish_latest,
    )
    report = dict(result.get("report") or {})
    print(t("compare.profile", value=args.profile))
    print(t("compare.scenario", value=result.get("scenario")))
    print(t("compare.report_dir", value=result.get("report_dir")))
    if result.get("report_json"):
        print(t("compare.report_json", value=result.get("report_json")))
    if result.get("report_markdown"):
        print(t("compare.report_markdown", value=result.get("report_markdown")))
    latest_indexes = result.get("latest_indexes") or {}
    if isinstance(latest_indexes, dict):
        for index_name, value in sorted(latest_indexes.items()):
            print(t("compare.latest_index", index_name=index_name, value=value))
    print(
        t(
            "compare.evidence_source",
            value=display_evidence_source(report.get("evidence_source"), default=str(report.get("evidence_source") or "--")),
        )
    )
    print(
        t(
            "compare.compare_status",
            value=display_compare_status(result.get("compare_status"), default=str(result.get("compare_status") or "--")),
        )
    )
    return 0 if str(result.get("compare_status")) == "MATCH" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
