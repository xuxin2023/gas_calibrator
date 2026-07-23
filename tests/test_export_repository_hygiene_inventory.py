from __future__ import annotations

from gas_calibrator.tools.export_repository_hygiene_inventory import (
    _branch_recommendation,
    _parse_worktree_porcelain,
    _worktree_recommendation,
)


def test_parse_worktree_porcelain_preserves_branch_and_detached_state() -> None:
    rows = _parse_worktree_porcelain(
        "\n".join(
            [
                "worktree D:/repo",
                "HEAD abc123",
                "branch refs/heads/main",
                "",
                "worktree D:/repo-detached",
                "HEAD def456",
                "detached",
                "prunable gitdir file points to non-existent location",
                "",
            ]
        )
    )

    assert rows == [
        {
            "worktree": "D:/repo",
            "HEAD": "abc123",
            "branch": "refs/heads/main",
        },
        {
            "worktree": "D:/repo-detached",
            "HEAD": "def456",
            "detached": True,
            "prunable": True,
            "prunable_reason": "gitdir file points to non-existent location",
        },
    ]


def test_branch_recommendation_never_deletes_checked_out_or_unpublished_branch() -> None:
    assert _branch_recommendation(
        checked_out=True,
        merged_into_target=True,
        upstream="origin/main",
    )[0] == "keep_checked_out"
    assert _branch_recommendation(
        checked_out=False,
        merged_into_target=False,
        upstream="",
    )[0] == "review_unpublished"


def test_worktree_recommendation_keeps_dirty_and_current_worktrees() -> None:
    common = {
        "exists": True,
        "detached": False,
        "prunable": False,
        "merged_into_target": True,
        "upstream": "origin/main",
    }
    assert _worktree_recommendation(is_current=True, dirty=True, **common)[0] == "keep_current"
    assert _worktree_recommendation(is_current=False, dirty=True, **common)[0] == "keep_dirty_review"
    assert (
        _worktree_recommendation(is_current=False, dirty=False, **common)[0]
        == "remove_after_confirmation"
    )
