from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

from ..core.cutover_candidate_worksheet import (
    CUTOVER_WORKSHEET_FILENAME,
    CUTOVER_WORKSHEET_MARKDOWN_FILENAME,
    FREEZE_CHECK_SUMMARY_FILENAME,
    ROLLBACK_GUARD_FILENAME,
    build_cutover_candidate_worksheet,
    collect_git_changed_paths,
    write_cutover_candidate_artifacts,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_output_dir(repo_root: Path) -> Path:
    return repo_root / "docs" / "architecture" / "v2_cutover_candidate"


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    repo_root = _repo_root()
    parser = argparse.ArgumentParser(
        description=(
            "Build the V2 cutover-candidate worksheet for real-machine dry-run "
            "preparation. This is offline/review only and does not authorize real write."
        )
    )
    parser.add_argument("--repo-root", default=str(repo_root), help="Repository root used for git freeze checks.")
    parser.add_argument("--base-ref", default="HEAD", help="Git base ref for changed-path freeze checks.")
    parser.add_argument(
        "--output-dir",
        default=str(_default_output_dir(repo_root)),
        help="Directory for worksheet JSON/Markdown, rollback guard, and freeze summary.",
    )
    parser.add_argument("--third-batch-cloud-commit", default="cba08beb")
    parser.add_argument("--third-batch-scope", default="narrowed_skip0_co2_only")
    parser.add_argument("--third-batch-conclusion", default="replacement-validation path usable")
    parser.add_argument(
        "--include-untracked",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include untracked files in the V1 freeze check, excluding local handoff scratch.",
    )
    parser.add_argument(
        "--changed-path",
        action="append",
        default=None,
        help="Override git changed-path collection; may be provided multiple times.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(str(args.repo_root)).resolve()
    if args.changed_path is None:
        changed_paths, ignored_paths = collect_git_changed_paths(
            repo_root,
            base_ref=str(args.base_ref),
            include_untracked=bool(args.include_untracked),
        )
    else:
        changed_paths = [str(path) for path in list(args.changed_path or [])]
        ignored_paths = []
    payload = build_cutover_candidate_worksheet(
        changed_paths=changed_paths,
        ignored_paths=ignored_paths,
        third_batch_cloud_commit=str(args.third_batch_cloud_commit),
        third_batch_scope=str(args.third_batch_scope),
        third_batch_conclusion=str(args.third_batch_conclusion),
        third_batch_cutover_ready=False,
    )
    written = write_cutover_candidate_artifacts(args.output_dir, payload)
    print(f"worksheet_json: {written['worksheet_json']}")
    print(f"worksheet_markdown: {written['worksheet_markdown']}")
    print(f"rollback_guard: {written['rollback_guard']}")
    print(f"freeze_check_summary: {written['freeze_check_summary']}")
    print(f"selected_conclusion: {payload['selected_conclusion']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
