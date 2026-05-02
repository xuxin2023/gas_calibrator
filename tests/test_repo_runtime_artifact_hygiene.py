from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ARTIFACT_PREFIXES = (
    "audit/real_pace_controller_acceptance/",
    "live_write_scan_",
    "offline_recompute_",
)
IGNORE_EXAMPLES = (
    "audit/real_pace_controller_acceptance/example",
    "live_write_scan_example",
    "offline_recompute_example",
)


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_runtime_artifact_paths_are_not_tracked() -> None:
    tracked_files = [
        line.strip()
        for line in _git("ls-files").stdout.splitlines()
        if line.startswith(RUNTIME_ARTIFACT_PREFIXES)
    ]

    assert tracked_files == []


def test_runtime_artifact_paths_are_ignored() -> None:
    result = _git("check-ignore", "-v", *IGNORE_EXAMPLES)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    assert len(lines) == len(IGNORE_EXAMPLES)
    assert any(
        ".gitignore:" in line
        and "audit/real_pace_controller_acceptance/" in line
        and "audit/real_pace_controller_acceptance/example" in line
        for line in lines
    )
    assert any(
        ".gitignore:" in line
        and "live_write_scan_" in line
        and "live_write_scan_example" in line
        for line in lines
    )
    assert any(
        ".gitignore:" in line
        and "offline_recompute_" in line
        and "offline_recompute_example" in line
        for line in lines
    )
