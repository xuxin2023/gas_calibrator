"""Export a read-only Git worktree and branch governance inventory."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence


SCHEMA = "repository_hygiene_inventory_v1"


def _git(repo_root: Path, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd or repo_root),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _parse_worktree_porcelain(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    for raw_line in [*text.splitlines(), ""]:
        line = raw_line.strip()
        if not line:
            if current:
                rows.append(current)
                current = {}
            continue
        key, _, value = line.partition(" ")
        if key in {"detached", "bare", "locked", "prunable"}:
            current[key] = True
            if value:
                current[f"{key}_reason"] = value
        else:
            current[key] = value
    return rows


def _parse_branch_rows(text: str) -> list[dict[str, str]]:
    fields = ("branch", "head", "upstream", "upstream_track", "last_commit_iso", "subject")
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        values = line.split("\t", maxsplit=len(fields) - 1)
        values.extend([""] * (len(fields) - len(values)))
        rows.append(dict(zip(fields, values, strict=True)))
    return rows


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _branch_recommendation(
    *,
    checked_out: bool,
    merged_into_target: bool,
    upstream: str,
) -> tuple[str, str]:
    if checked_out:
        return "keep_checked_out", "branch is attached to a worktree"
    if merged_into_target:
        return "archive_delete_after_confirmation", "branch is merged into the review target"
    if not upstream:
        return "review_unpublished", "branch has no upstream and may contain local-only work"
    return "keep_unmerged", "branch is not merged into the review target"


def _worktree_recommendation(
    *,
    is_current: bool,
    exists: bool,
    dirty: bool,
    detached: bool,
    prunable: bool,
    merged_into_target: bool,
    upstream: str,
) -> tuple[str, str]:
    if is_current:
        return "keep_current", "current audit worktree"
    if not exists or prunable:
        return "remove_after_confirmation", "worktree path is missing or Git marks it prunable"
    if dirty:
        return "keep_dirty_review", "worktree contains tracked or untracked changes"
    if detached:
        return "archive_review", "clean detached worktree may preserve unreferenced evidence"
    if merged_into_target:
        return "remove_after_confirmation", "clean worktree branch is merged into the review target"
    if not upstream:
        return "keep_unpublished_review", "clean worktree branch has no upstream"
    return "keep_unmerged", "clean worktree branch is not merged into the review target"


def _count_by(rows: Iterable[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key, "")) for row in rows).items()))


def build_inventory(
    *,
    repo_root: Path,
    target_ref: str,
    current_worktree: Path,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    current_worktree = current_worktree.resolve()

    worktree_result = _git(repo_root, "worktree", "list", "--porcelain")
    if worktree_result.returncode != 0:
        raise RuntimeError(worktree_result.stderr.strip() or "git worktree list failed")
    raw_worktrees = _parse_worktree_porcelain(worktree_result.stdout)

    branch_format = (
        "%(refname:short)%09%(objectname)%09%(upstream:short)%09"
        "%(upstream:track)%09%(committerdate:iso-strict)%09%(subject)"
    )
    branch_result = _git(
        repo_root,
        "for-each-ref",
        "refs/heads",
        f"--format={branch_format}",
    )
    if branch_result.returncode != 0:
        raise RuntimeError(branch_result.stderr.strip() or "git for-each-ref failed")
    branch_rows = _parse_branch_rows(branch_result.stdout)

    merged_result = _git(
        repo_root,
        "for-each-ref",
        "refs/heads",
        f"--merged={target_ref}",
        "--format=%(refname:short)",
    )
    merged_branches = (
        {line.strip() for line in merged_result.stdout.splitlines() if line.strip()}
        if merged_result.returncode == 0
        else set()
    )

    checked_out_by_branch: dict[str, list[str]] = {}
    for row in raw_worktrees:
        branch_ref = str(row.get("branch", ""))
        if branch_ref.startswith("refs/heads/"):
            branch = branch_ref.removeprefix("refs/heads/")
            checked_out_by_branch.setdefault(branch, []).append(str(row.get("worktree", "")))

    branches: list[dict[str, Any]] = []
    branch_lookup: dict[str, dict[str, Any]] = {}
    for row in branch_rows:
        branch = row["branch"]
        checked_paths = checked_out_by_branch.get(branch, [])
        merged = branch in merged_branches
        recommendation, reason = _branch_recommendation(
            checked_out=bool(checked_paths),
            merged_into_target=merged,
            upstream=row["upstream"],
        )
        branch_row: dict[str, Any] = {
            **row,
            "checked_out": bool(checked_paths),
            "worktree_paths": " | ".join(checked_paths),
            "merged_into_target": merged,
            "target_ref": target_ref,
            "recommendation": recommendation,
            "recommendation_reason": reason,
        }
        branches.append(branch_row)
        branch_lookup[branch] = branch_row

    worktrees: list[dict[str, Any]] = []
    for row in raw_worktrees:
        worktree_path = Path(str(row.get("worktree", "")))
        exists = worktree_path.exists()
        status_result = (
            _git(repo_root, "status", "--porcelain", "--untracked-files=normal", cwd=worktree_path)
            if exists
            else None
        )
        status_lines = (
            [line for line in status_result.stdout.splitlines() if line.strip()]
            if status_result is not None and status_result.returncode == 0
            else []
        )
        branch_ref = str(row.get("branch", ""))
        branch = branch_ref.removeprefix("refs/heads/") if branch_ref else ""
        branch_meta = branch_lookup.get(branch, {})
        is_current = exists and worktree_path.resolve() == current_worktree
        recommendation, reason = _worktree_recommendation(
            is_current=is_current,
            exists=exists,
            dirty=bool(status_lines),
            detached=bool(row.get("detached")),
            prunable=bool(row.get("prunable")),
            merged_into_target=bool(branch_meta.get("merged_into_target")),
            upstream=str(branch_meta.get("upstream", "")),
        )
        worktrees.append(
            {
                "path": str(worktree_path),
                "exists": exists,
                "head": str(row.get("HEAD", "")),
                "branch": branch,
                "detached": bool(row.get("detached")),
                "locked": bool(row.get("locked")),
                "prunable": bool(row.get("prunable")),
                "is_current": is_current,
                "dirty": bool(status_lines),
                "dirty_entry_count": len(status_lines),
                "upstream": str(branch_meta.get("upstream", "")),
                "merged_into_target": bool(branch_meta.get("merged_into_target")),
                "target_ref": target_ref,
                "last_commit_iso": str(branch_meta.get("last_commit_iso", "")),
                "subject": str(branch_meta.get("subject", "")),
                "recommendation": recommendation,
                "recommendation_reason": reason,
            }
        )

    return {
        "schema": SCHEMA,
        "generated_at": datetime.now().astimezone().isoformat(),
        "repo_root": str(repo_root),
        "current_worktree": str(current_worktree),
        "target_ref": target_ref,
        "worktrees": worktrees,
        "branches": branches,
        "summary": {
            "worktree_count": len(worktrees),
            "dirty_worktree_count": sum(bool(row["dirty"]) for row in worktrees),
            "detached_worktree_count": sum(bool(row["detached"]) for row in worktrees),
            "worktree_recommendations": _count_by(worktrees, "recommendation"),
            "branch_count": len(branches),
            "branch_without_upstream_count": sum(not bool(row["upstream"]) for row in branches),
            "merged_branch_count": sum(bool(row["merged_into_target"]) for row in branches),
            "branch_recommendations": _count_by(branches, "recommendation"),
        },
    }


def write_inventory(payload: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    worktree_fields = (
        "path",
        "exists",
        "head",
        "branch",
        "detached",
        "locked",
        "prunable",
        "is_current",
        "dirty",
        "dirty_entry_count",
        "upstream",
        "merged_into_target",
        "target_ref",
        "last_commit_iso",
        "subject",
        "recommendation",
        "recommendation_reason",
    )
    branch_fields = (
        "branch",
        "head",
        "upstream",
        "upstream_track",
        "last_commit_iso",
        "subject",
        "checked_out",
        "worktree_paths",
        "merged_into_target",
        "target_ref",
        "recommendation",
        "recommendation_reason",
    )
    worktree_csv = output_dir / "worktrees.csv"
    branch_csv = output_dir / "branches.csv"
    summary_json = output_dir / "summary.json"
    summary_md = output_dir / "summary.md"
    _write_csv(worktree_csv, payload["worktrees"], worktree_fields)
    _write_csv(branch_csv, payload["branches"], branch_fields)
    summary_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = payload["summary"]
    summary_md.write_text(
        "\n".join(
            [
                "# Repository Hygiene Inventory",
                "",
                f"- Generated: `{payload['generated_at']}`",
                f"- Repository: `{payload['repo_root']}`",
                f"- Review target: `{payload['target_ref']}`",
                f"- Worktrees: `{summary['worktree_count']}`",
                f"- Dirty worktrees: `{summary['dirty_worktree_count']}`",
                f"- Detached worktrees: `{summary['detached_worktree_count']}`",
                f"- Local branches: `{summary['branch_count']}`",
                f"- Branches without upstream: `{summary['branch_without_upstream_count']}`",
                f"- Branches merged into target: `{summary['merged_branch_count']}`",
                "",
                "## Worktree Recommendations",
                "",
                *[
                    f"- `{name}`: {count}"
                    for name, count in summary["worktree_recommendations"].items()
                ],
                "",
                "## Branch Recommendations",
                "",
                *[
                    f"- `{name}`: {count}"
                    for name, count in summary["branch_recommendations"].items()
                ],
                "",
                "This inventory is read-only. Every remove/delete recommendation requires review and confirmation.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "worktrees_csv": str(worktree_csv),
        "branches_csv": str(branch_csv),
        "summary_json": str(summary_json),
        "summary_md": str(summary_md),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--current-worktree", type=Path, default=Path.cwd())
    parser.add_argument("--target-ref", default="origin/main")
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_inventory(
        repo_root=args.repo_root,
        target_ref=args.target_ref,
        current_worktree=args.current_worktree,
    )
    outputs = write_inventory(payload, args.output_dir)
    print(
        json.dumps(
            {
                "status": "ok",
                "schema": SCHEMA,
                "summary": payload["summary"],
                "outputs": outputs,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
