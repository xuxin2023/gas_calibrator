from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from ..core.regression_scoreboard import (
    ARTIFACT_SCHEMA_DIFF_FILENAME,
    BUNDLE_DIFF_SUMMARY_FILENAME,
    REGRESSION_SCOREBOARD_FILENAME,
    REGRESSION_SCOREBOARD_MARKDOWN_FILENAME,
    generate_regression_scoreboard,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build reviewer-only regression scoreboard for offline/simulation/replay bundles."
    )
    parser.add_argument("--current-bundle-dir", required=True, help="Current bundle directory to score.")
    parser.add_argument("--baseline-bundle-dir", default=None, help="Optional previous baseline bundle directory.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where regression_scoreboard.json/md and diff artifacts will be written.",
    )
    parser.add_argument("--current-label", default="current_branch_result", help="Label for current bundle.")
    parser.add_argument("--baseline-label", default="previous_baseline", help="Label for baseline bundle.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    scoreboard = generate_regression_scoreboard(
        current_bundle_dir=Path(str(args.current_bundle_dir)).resolve(),
        baseline_bundle_dir=Path(str(args.baseline_bundle_dir)).resolve() if args.baseline_bundle_dir else None,
        output_dir=Path(str(args.output_dir)).resolve(),
        current_label=str(args.current_label),
        baseline_label=str(args.baseline_label),
    )
    output_dir = Path(str(args.output_dir)).resolve()
    print(f"Regression scoreboard: {output_dir / REGRESSION_SCOREBOARD_FILENAME}")
    print(f"Regression markdown: {output_dir / REGRESSION_SCOREBOARD_MARKDOWN_FILENAME}")
    print(f"Bundle diff summary: {output_dir / BUNDLE_DIFF_SUMMARY_FILENAME}")
    print(f"Artifact schema diff: {output_dir / ARTIFACT_SCHEMA_DIFF_FILENAME}")
    recommendation = dict(scoreboard.get("recommendation") or {})
    print(
        f"Recommended Step 2 baseline bundle: {recommendation.get('recommended_bundle_label')} "
        f"({recommendation.get('recommendation_state')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
