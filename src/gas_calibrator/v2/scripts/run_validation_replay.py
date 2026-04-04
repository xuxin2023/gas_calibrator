from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..sim import DEFAULT_REPLAY_FIXTURE_ROOT, list_replay_scenarios, load_replay_fixture, materialize_replay_fixture


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline validation replay from a fixture or named scenario.")
    parser.add_argument("--scenario", default=None, help="Scenario name under tests/v2/fixtures/replay.")
    parser.add_argument("--fixture", default=None, help="Explicit replay fixture json path.")
    parser.add_argument(
        "--report-root",
        default=str(Path(__file__).resolve().parents[2] / "output" / "v1_v2_compare"),
        help="Where replay artifacts should be written.",
    )
    parser.add_argument("--run-name", default=None, help="Optional fixed replay run name.")
    parser.add_argument(
        "--publish-latest",
        action="store_true",
        help="Also refresh the report-root latest index. Default is off so replay does not overwrite real bench evidence.",
    )
    parser.add_argument("--list-scenarios", action="store_true", help="List available built-in replay scenarios.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.list_scenarios:
        for name in list_replay_scenarios():
            print(name)
        return 0
    payload = load_replay_fixture(scenario=args.scenario, fixture=args.fixture, root=DEFAULT_REPLAY_FIXTURE_ROOT)
    result = materialize_replay_fixture(
        payload,
        report_root=Path(str(args.report_root)).resolve(),
        run_name=args.run_name,
        publish_latest=bool(args.publish_latest),
        evidence_state_override="replay",
    )
    print(f"Scenario: {result.get('scenario')}")
    print(f"Report dir: {result.get('report_dir')}")
    if result.get("report_json"):
        print(f"JSON report: {result.get('report_json')}")
    if result.get("report_markdown"):
        print(f"Markdown report: {result.get('report_markdown')}")
    latest_indexes = result.get("latest_indexes") or {}
    if isinstance(latest_indexes, dict):
        for key, value in sorted(latest_indexes.items()):
            print(f"{key}: {value}")
    elif isinstance(latest_indexes, list):
        for value in latest_indexes:
            print(f"latest_index: {value}")
    print(f"Compare status: {result.get('compare_status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
