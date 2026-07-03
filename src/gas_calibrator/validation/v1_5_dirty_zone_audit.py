"""Offline dirty-zone audit for the V1.5 consolidation workspace.

The audit reads ``git status --short`` output and classifies files by where they
belong. It never deletes, stages, unstages, or rewrites files. Its purpose is to
keep the clean V1.5 worktree, historical handoff evidence, and the polluted root
workspace from being mixed into one formal package.
"""

from __future__ import annotations

import csv
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .v1_5_entrypoint_inventory import classify_v1_5_entrypoint


SCHEMA = "v1_5_dirty_zone_audit_v1"

FORBIDDEN_CLEAN_STAGED_ENTRYPOINT_CATEGORIES = {
    "controlled_write",
    "diagnostic_only",
    "formal_sampling_worker",
    "housekeeping_archive",
    "legacy_v1_reference",
    "unclassified_v1_5_tool",
}


@dataclass(frozen=True)
class DirtyZoneEntry:
    workspace: str
    status_code: str
    path: str
    staged: bool
    unstaged: bool
    untracked: bool
    category: str
    severity: str
    action: str
    allowed_in_v1_5_package: bool
    reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DirtyZoneAudit:
    schema: str
    created_at: str
    status: str
    clean_worktree: str
    root_workspace: str
    summary: Mapping[str, Any]
    policy: Mapping[str, Any]
    entries: tuple[DirtyZoneEntry, ...]

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["entries"] = [entry.to_json() for entry in self.entries]
        return payload


def _norm_path(path: str) -> str:
    return str(path or "").strip().replace("\\", "/")


def _parse_status_line(line: str) -> tuple[str, str] | None:
    if not line.strip():
        return None
    if len(line) < 3:
        return None
    status_code = line[:2]
    path = line[3:].strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[-1].strip()
    return status_code, _norm_path(path)


def _status_flags(status_code: str) -> tuple[bool, bool, bool]:
    if status_code == "??":
        return False, False, True
    left = status_code[0] if len(status_code) > 0 else " "
    right = status_code[1] if len(status_code) > 1 else " "
    return left not in {" ", "?"}, right not in {" ", "?"}, False


def _looks_like_v2_surface(path: str) -> bool:
    lower = path.lower()
    name = Path(path).stem.lower()
    return (
        lower.startswith("src/gas_calibrator/v2/")
        or lower.startswith("tests/v2/")
        or name.startswith("run_v2_")
        or name.startswith("export_v2_")
        or name.startswith("import_v2_")
    )


def _looks_like_legacy_v1_surface(path: str) -> bool:
    name = Path(path).stem.lower()
    return path.lower().startswith("src/gas_calibrator/tools/") and name.startswith("run_v1_") and not name.startswith(
        "run_v1_5_"
    )


def _looks_like_tool_entrypoint(path: str) -> bool:
    return path.lower().startswith("src/gas_calibrator/tools/")


def _clean_staged_blocker(path: str) -> tuple[str, str, str] | None:
    normalized = _norm_path(path)
    if normalized.startswith("_handoff/"):
        return (
            "clean_staged_handoff_blocker",
            "unstage_keep_as_traceability_evidence",
            "Handoff evidence must stay out of formal code packages even when it was accidentally staged.",
        )
    if _looks_like_v2_surface(normalized):
        return (
            "clean_staged_v2_surface_blocker",
            "unstage_or_move_to_v2_review_package",
            "V2 surfaces are not part of the V1.5 formal package and must not be mixed into this clean-worktree commit.",
        )
    if _looks_like_legacy_v1_surface(normalized):
        return (
            "clean_staged_legacy_v1_entrypoint_blocker",
            "unstage_keep_as_historical_reference",
            "Legacy V1 entrypoints are reference-only and must not start or alter the V1.5 formal flow.",
        )

    entry = classify_v1_5_entrypoint(Path(normalized), root=Path.cwd()) if _looks_like_tool_entrypoint(normalized) else None
    if entry is not None and entry.category in FORBIDDEN_CLEAN_STAGED_ENTRYPOINT_CATEGORIES:
        return (
            "clean_staged_noncanonical_entrypoint_blocker",
            "unstage_or_split_into_explicit_review_package",
            (
                f"Staged entrypoint category {entry.category!r} is not allowed in a formal V1.5 package; "
                "use the canonical owner or a separately authorized review path."
            ),
        )
    return None


def classify_dirty_zone_entry(
    *,
    workspace: str,
    status_code: str,
    path: str,
) -> DirtyZoneEntry:
    normalized = _norm_path(path)
    staged, unstaged, untracked = _status_flags(status_code)
    workspace_key = str(workspace or "").strip().lower()

    if workspace_key == "clean_worktree":
        clean_blocker = _clean_staged_blocker(normalized) if staged else None
        if clean_blocker is not None:
            category, action, reason = clean_blocker
            severity = "blocker"
            allowed = False
        elif untracked and normalized.startswith("_handoff/"):
            category = "clean_handoff_evidence_retained"
            severity = "info"
            action = "keep_untracked_do_not_stage_into_code_package"
            allowed = False
            reason = "Historical V1.5 evidence is retained for traceability but must not be bundled with code changes."
        elif staged:
            category = "clean_staged_candidate_package"
            severity = "review"
            action = "verify_small_package_scope_before_commit"
            allowed = True
            reason = "Staged clean-worktree changes are only acceptable as a reviewed small package."
        elif untracked:
            category = "clean_untracked_review_required"
            severity = "review"
            action = "classify_before_stage_or_leave_untracked"
            allowed = False
            reason = "Untracked clean-worktree files need explicit classification before they can enter V1.5."
        else:
            category = "clean_tracked_change_review_required"
            severity = "review"
            action = "review_scope_before_stage_or_commit"
            allowed = True
            reason = "Tracked clean-worktree changes may be a work-in-progress package and need scoped review."
    else:
        if staged:
            category = "root_staged_pollution_blocker"
            severity = "blocker"
            action = "do_not_commit_or_use_for_v1_5_release"
            allowed = False
            reason = "The root workspace is isolated as a draft/pollution zone; staged root changes must not enter V1.5."
        elif normalized.startswith(".codex/") or normalized.startswith(".playwright-mcp/"):
            category = "root_tool_state_ignore"
            severity = "info"
            action = "ignore_for_v1_5_flow"
            allowed = False
            reason = "Local tool state is not V1.5 calibration code or evidence."
        elif untracked and normalized.startswith("_handoff/"):
            category = "root_handoff_evidence_retained"
            severity = "info"
            action = "keep_as_historical_evidence_do_not_promote"
            allowed = False
            reason = "Root handoff artifacts are traceability/draft evidence and remain outside the clean V1.5 package."
        elif untracked:
            category = "root_untracked_draft_isolated"
            severity = "warning"
            action = "leave_isolated_until_reviewed_for_clean_worktree_migration"
            allowed = False
            reason = "Root untracked files may be drafts; migrate only by explicit review into the clean worktree."
        else:
            category = "root_tracked_dirty_isolated"
            severity = "warning"
            action = "do_not_use_root_as_formal_v1_5_source"
            allowed = False
            reason = "Root tracked dirty files are isolated from the official V1.5 clean worktree."

    return DirtyZoneEntry(
        workspace=workspace_key or "unknown",
        status_code=status_code,
        path=normalized,
        staged=staged,
        unstaged=unstaged,
        untracked=untracked,
        category=category,
        severity=severity,
        action=action,
        allowed_in_v1_5_package=allowed,
        reason=reason,
    )


def parse_git_status_short(status_text: str, *, workspace: str) -> tuple[DirtyZoneEntry, ...]:
    entries: list[DirtyZoneEntry] = []
    for line in str(status_text or "").splitlines():
        parsed = _parse_status_line(line)
        if parsed is None:
            continue
        status_code, path = parsed
        entries.append(classify_dirty_zone_entry(workspace=workspace, status_code=status_code, path=path))
    return tuple(entries)


def _run_git_status_short(workspace: Path) -> str:
    result = subprocess.run(
        ["git", "status", "--short"],
        cwd=str(workspace.resolve()),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git status failed in {workspace}: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def _summary(entries: Sequence[DirtyZoneEntry]) -> dict[str, Any]:
    by_category: dict[str, int] = {}
    by_severity: dict[str, int] = {}
    by_workspace: dict[str, int] = {}
    for entry in entries:
        by_category[entry.category] = by_category.get(entry.category, 0) + 1
        by_severity[entry.severity] = by_severity.get(entry.severity, 0) + 1
        by_workspace[entry.workspace] = by_workspace.get(entry.workspace, 0) + 1
    return {
        "entry_count": len(entries),
        "by_category": dict(sorted(by_category.items())),
        "by_severity": dict(sorted(by_severity.items())),
        "by_workspace": dict(sorted(by_workspace.items())),
        "blocker_count": by_severity.get("blocker", 0),
        "warning_count": by_severity.get("warning", 0),
        "review_count": by_severity.get("review", 0),
        "info_count": by_severity.get("info", 0),
    }


def build_dirty_zone_audit(
    *,
    clean_worktree: str | Path,
    root_workspace: str | Path | None = None,
    clean_status_text: str | None = None,
    root_status_text: str | None = None,
) -> DirtyZoneAudit:
    clean_root = Path(clean_worktree).resolve()
    root_root = Path(root_workspace).resolve() if root_workspace else clean_root
    clean_status = clean_status_text if clean_status_text is not None else _run_git_status_short(clean_root)
    root_status = root_status_text if root_status_text is not None else _run_git_status_short(root_root)
    entries = (
        *parse_git_status_short(clean_status, workspace="clean_worktree"),
        *parse_git_status_short(root_status, workspace="root_workspace"),
    )
    summary = _summary(entries)
    if summary["blocker_count"]:
        status = "blocked"
    elif summary["warning_count"] or summary["review_count"]:
        status = "review_required"
    else:
        status = "clean_or_evidence_only"

    return DirtyZoneAudit(
        schema=SCHEMA,
        created_at=datetime.now().isoformat(timespec="seconds"),
        status=status,
        clean_worktree=str(clean_root),
        root_workspace=str(root_root),
        summary=summary,
        policy={
            "root_workspace_policy": "isolated_draft_pollution_zone_not_formal_source",
            "clean_worktree_policy": "only_small_reviewed_packages_may_be_staged_or_committed",
            "handoff_policy": "retain_as_traceability_evidence_but_do_not_stage_into_code_packages",
            "destructive_actions_allowed": False,
            "opens_com_ports": False,
            "writes_files_outside_output_dir": False,
        },
        entries=tuple(entries),
    )


def render_dirty_zone_audit_markdown(audit: DirtyZoneAudit) -> str:
    lines = [
        "# V1.5 Dirty Zone Audit",
        "",
        f"- schema: `{audit.schema}`",
        f"- status: `{audit.status}`",
        f"- clean_worktree: `{audit.clean_worktree}`",
        f"- root_workspace: `{audit.root_workspace}`",
        "",
        "## Policy",
        "",
    ]
    for key, value in audit.policy.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- entries: `{audit.summary['entry_count']}`",
            f"- blockers: `{audit.summary['blocker_count']}`",
            f"- warnings: `{audit.summary['warning_count']}`",
            f"- reviews: `{audit.summary['review_count']}`",
            f"- info: `{audit.summary['info_count']}`",
            "",
            "## Entries",
            "",
            "| Workspace | Severity | Category | Status | Path | Action |",
            "|---|---|---|---|---|---|",
        ]
    )
    if audit.entries:
        for entry in audit.entries:
            lines.append(
                f"| `{entry.workspace}` | `{entry.severity}` | `{entry.category}` | "
                f"`{entry.status_code}` | `{entry.path}` | {entry.action} |"
            )
    else:
        lines.append("|  | `clean` |  |  |  | no dirty or untracked entries detected |")
    return "\n".join(lines).rstrip() + "\n"


def write_dirty_zone_audit(audit: DirtyZoneAudit, output_dir: str | Path) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_dirty_zone_audit.json"
    md_path = root / "v1_5_dirty_zone_audit.md"
    csv_path = root / "v1_5_dirty_zone_entries.csv"

    json_path.write_text(json.dumps(audit.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_dirty_zone_audit_markdown(audit), encoding="utf-8")
    rows = [entry.to_json() for entry in audit.entries]
    header = [
        "workspace",
        "status_code",
        "path",
        "staged",
        "unstaged",
        "untracked",
        "category",
        "severity",
        "action",
        "allowed_in_v1_5_package",
        "reason",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    return {"json": json_path, "markdown": md_path, "csv": csv_path}
