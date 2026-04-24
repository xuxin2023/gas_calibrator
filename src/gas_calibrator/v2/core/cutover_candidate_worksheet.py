from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence


CUTOVER_WORKSHEET_FILENAME = "v2_cutover_candidate_worksheet.json"
CUTOVER_WORKSHEET_MARKDOWN_FILENAME = "v2_cutover_candidate_worksheet.md"
ROLLBACK_GUARD_FILENAME = "v2_cutover_rollback_guard.md"
FREEZE_CHECK_SUMMARY_FILENAME = "v1_freeze_check_summary.json"

WORKSHEET_SCHEMA_VERSION = "1.0"
WORKSHEET_ARTIFACT_TYPE = "v2_cutover_candidate_worksheet"
ROLLBACK_GUARD_ARTIFACT_TYPE = "v2_cutover_rollback_guard"
FREEZE_CHECK_ARTIFACT_TYPE = "v1_freeze_check_summary"

STATUS_GREEN = "green"
STATUS_YELLOW = "yellow"
STATUS_RED = "red"
VALID_STATUSES = {STATUS_GREEN, STATUS_YELLOW, STATUS_RED}

CONCLUSION_DRY_RUN_PREP_ALLOWED = "can_enter_v2_real_machine_dry_run_preparation_no_real_write"
CONCLUSION_P1_FIRST = "complete_minor_p1_before_v2_real_machine_dry_run_preparation"
CONCLUSION_P0_BLOCKED = "not_recommended_for_v2_real_machine_dry_run_preparation_p0_blocked"

CONCLUSION_LABELS = {
    CONCLUSION_DRY_RUN_PREP_ALLOWED: (
        "Can enter V2 real-machine dry-run preparation, but real write remains prohibited."
    ),
    CONCLUSION_P1_FIRST: (
        "Complete minor P1 items before entering V2 real-machine dry-run preparation."
    ),
    CONCLUSION_P0_BLOCKED: (
        "Do not enter V2 real-machine dry-run preparation; P0 blockers remain."
    ),
}

FORBIDDEN_V1_CHANGE_RULES: tuple[tuple[str, str], ...] = (
    ("exact", "run_app.py"),
    ("prefix", "src/gas_calibrator/workflow/"),
    ("prefix", "src/gas_calibrator/ui/"),
    ("prefix", "src/gas_calibrator/devices/"),
    ("contains", "/v1/"),
    ("contains", "/legacy/"),
)

DEFAULT_IGNORED_UNTRACKED_PREFIXES = ("_handoff/",)


@dataclass(frozen=True)
class ReadinessSeed:
    key: str
    label: str
    status: str
    evidence_path: str
    reason: str
    remaining_blocker: str
    blocks_dry_run_preparation: bool = False


READINESS_SEEDS: tuple[ReadinessSeed, ...] = (
    ReadinessSeed(
        key="smoke",
        label="Smoke",
        status=STATUS_GREEN,
        evidence_path="tests/v2/test_run_validation_replay.py; tests/v2/test_compare_v1_v2_control_flow.py",
        reason="Offline replay and compare smoke coverage is available for the current preparation worksheet.",
        remaining_blocker="No dry-run-preparation blocker; real smoke remains explicitly out of scope.",
    ),
    ReadinessSeed(
        key="safe",
        label="Safe",
        status=STATUS_GREEN,
        evidence_path="src/gas_calibrator/v2/scripts/_cli_safety.py; tests/v2/test_export_resilience.py",
        reason="Step-2 safety messaging and export resilience are available without opening real ports.",
        remaining_blocker="No dry-run-preparation blocker; real COM/serial and live write remain prohibited.",
    ),
    ReadinessSeed(
        key="route_trace",
        label="route_trace",
        status=STATUS_GREEN,
        evidence_path="src/gas_calibrator/v2/scripts/route_trace_diff.py",
        reason="Route traces are produced and diffable as offline review artifacts.",
        remaining_blocker="Real route trace refresh is not allowed in this batch.",
    ),
    ReadinessSeed(
        key="fit_ready",
        label="Fit-Ready",
        status=STATUS_GREEN,
        evidence_path="src/gas_calibrator/v2/output/fit_ready_smoke/run_*; tests/v2/test_export_resilience.py",
        reason="Fit-ready/export artifact paths are covered as offline evidence and resilience tests.",
        remaining_blocker="No real fit-ready acceptance is claimed.",
    ),
    ReadinessSeed(
        key="completed_progress_semantics",
        label="completed/progress semantics",
        status=STATUS_GREEN,
        evidence_path="tests/v2/test_run_validation_replay.py; tests/v2/test_run_state.py",
        reason="Replay/state coverage protects completed and progress semantics from false-green drift.",
        remaining_blocker="No dry-run-preparation blocker.",
    ),
    ReadinessSeed(
        key="headless_entry",
        label="headless entry",
        status=STATUS_GREEN,
        evidence_path="src/gas_calibrator/v2/scripts/run_v2.py; tests/v2/test_run_v2.py",
        reason="The formal V2 headless entry remains the offline launch path for review and simulation.",
        remaining_blocker="Default production entry remains V1; no V2 default switch is allowed.",
    ),
    ReadinessSeed(
        key="h2o_single_route_readiness",
        label="H2O single-route readiness",
        status=STATUS_YELLOW,
        evidence_path=(
            "src/gas_calibrator/v2/output/v1_v2_compare/replacement_h2o_only_simulated_latest.json; "
            "tests/v2/test_verify_v1_v2_h2o_only_replacement.py"
        ),
        reason="H2O has diagnostic/offline coverage, but it is not the main replacement route for this phase.",
        remaining_blocker=(
            "H2O hardware equivalence and humidity-feedback behavior remain future dry-run/bench questions."
        ),
    ),
    ReadinessSeed(
        key="co2_single_route_readiness",
        label="CO2 single-route readiness",
        status=STATUS_GREEN,
        evidence_path=(
            "src/gas_calibrator/v2/output/v1_v2_compare/"
            "replacement_skip0_co2_only_simulated_latest.json"
        ),
        reason=(
            "The narrowed CO2-only skip0 path is usable for replacement-validation preparation in simulation."
        ),
        remaining_blocker="Real-machine dry-run evidence remains pending and must not include live write.",
    ),
    ReadinessSeed(
        key="single_temperature_group_readiness",
        label="single temperature group readiness",
        status=STATUS_YELLOW,
        evidence_path="src/gas_calibrator/v2/configs/validation/simulated/replacement_skip0_co2_only_simulated.json",
        reason="The current preparation path is narrowed to one CO2 temperature group, not a full H2O+CO2 group.",
        remaining_blocker="Full single-temperature H2O+CO2 group evidence remains future dry-run preparation work.",
    ),
    ReadinessSeed(
        key="route_trace_diff_readiness",
        label="route trace diff readiness",
        status=STATUS_GREEN,
        evidence_path="src/gas_calibrator/v2/scripts/route_trace_diff.py; tests/v2/test_route_trace_diff.py",
        reason="Route trace diff has structured output and tests for the narrowed replacement path.",
        remaining_blocker="Real compare/verify remains prohibited.",
    ),
    ReadinessSeed(
        key="narrowed_skip0_replacement_readiness",
        label="narrowed skip0 replacement readiness",
        status=STATUS_GREEN,
        evidence_path=(
            "src/gas_calibrator/v2/output/v1_v2_compare/"
            "replacement_skip0_co2_only_simulated_latest.json"
        ),
        reason="Third-batch scope is narrowed_skip0_co2_only and the path is usable for preparation.",
        remaining_blocker="cutover_ready remains false; this does not prove full V1 replacement.",
    ),
    ReadinessSeed(
        key="rollback_strategy_ready",
        label="rollback strategy ready",
        status=STATUS_GREEN,
        evidence_path=ROLLBACK_GUARD_FILENAME,
        reason="A fallback guard/SOP is generated with V1 as the unchanged default entry.",
        remaining_blocker="Dry-run rollback rehearsal remains document-only in this batch.",
    ),
)


def normalize_repo_path(path: str | Path) -> str:
    text = str(path).replace("\\", "/").strip()
    while text.startswith("./"):
        text = text[2:]
    return text


def is_forbidden_v1_change(path: str | Path) -> bool:
    normalized = normalize_repo_path(path)
    for rule, pattern in FORBIDDEN_V1_CHANGE_RULES:
        if rule == "exact" and normalized == pattern:
            return True
        if rule == "prefix" and normalized.startswith(pattern):
            return True
        if rule == "contains" and pattern in f"/{normalized}":
            return True
    return False


def build_v1_freeze_check(
    changed_paths: Iterable[str | Path],
    *,
    ignored_paths: Iterable[str | Path] | None = None,
) -> dict[str, object]:
    changed = sorted({normalize_repo_path(path) for path in changed_paths if str(path).strip()})
    ignored = sorted({normalize_repo_path(path) for path in (ignored_paths or []) if str(path).strip()})
    forbidden = [path for path in changed if is_forbidden_v1_change(path)]
    status = STATUS_RED if forbidden else STATUS_GREEN
    return {
        "schema_version": WORKSHEET_SCHEMA_VERSION,
        "artifact_type": FREEZE_CHECK_ARTIFACT_TYPE,
        "status": status,
        "evidence_source": "git_diff",
        "not_real_acceptance_evidence": True,
        "checked_rules": [
            "run_app.py unchanged",
            "src/gas_calibrator/workflow/** unchanged",
            "src/gas_calibrator/ui/** unchanged",
            "src/gas_calibrator/devices/** unchanged",
            "no /v1/ or /legacy/ path changed",
        ],
        "changed_paths": changed,
        "ignored_paths": ignored,
        "forbidden_changed_paths": forbidden,
        "reason": (
            "No V1 production/default-entry paths changed."
            if not forbidden
            else "Forbidden V1/default-entry paths changed."
        ),
        "remaining_blocker": "none" if not forbidden else "; ".join(forbidden),
        "v1_remains_frozen": not forbidden,
    }


def build_rollback_guard() -> dict[str, object]:
    return {
        "schema_version": WORKSHEET_SCHEMA_VERSION,
        "artifact_type": ROLLBACK_GUARD_ARTIFACT_TYPE,
        "stage": "v2_real_machine_dry_run_preparation",
        "default_entry": {
            "status": "v1_remains_default",
            "path": "run_app.py",
            "change_allowed": False,
            "reason": "The fourth batch does not switch the default production entry to V2.",
        },
        "future_dry_run_boundary": {
            "real_write_allowed": False,
            "real_com_open_allowed_in_this_batch": False,
            "real_acceptance_allowed_in_this_batch": False,
            "real_primary_latest_refresh_allowed": False,
            "operator_manual_device_control_allowed": False,
        },
        "preserve_v1_baselines": [
            "Current V1 run logs for the production path.",
            "Current V1 exported summaries, raw rows, and coefficient/writeback references.",
            "Current V1 runtime configuration and points source used for comparison.",
            "Known-good V1 sidecar/latest metadata, without rewriting real_primary_latest.",
        ],
        "rollback_sensitive_files": [
            "run_app.py",
            "src/gas_calibrator/workflow/**",
            "src/gas_calibrator/ui/**",
            "src/gas_calibrator/devices/**",
            "any /v1/ or /legacy/ path",
            "real_primary_latest and any real latest pointer",
        ],
        "rollback_triggers": [
            "Any V2 dry-run preparation attempts to open real write paths.",
            "Any coefficient, zero, span, or calibration-parameter write is attempted.",
            "V2 route trace enters an unexpected route or cleanup cannot restore safe state.",
            "V2 dry-run preparation produces false completed/progress semantics.",
            "V1 baseline outputs, logs, or configs are missing before a dry-run window.",
            "Any default-entry or V1 production path diff appears in review.",
        ],
        "rollback_steps": [
            "Stop the V2 preparation path and do not retry against real devices.",
            "Keep run_app.py pointed at the existing V1 default entry.",
            "Use the preserved V1 configuration, logs, and output baseline as the operating reference.",
            "Discard or quarantine the failed V2 dry-run artifacts as diagnostic-only evidence.",
            "Record the trigger, artifact paths, and operator/reviewer notes in the worksheet.",
            "Resume only the known-good V1 production workflow after V1 baseline verification passes.",
        ],
        "post_rollback_verification": [
            "git diff shows no changes to run_app.py or V1 workflow/ui/devices paths.",
            "V1 baseline config and points source are still present.",
            "V1 output/log storage path is still writable by the existing production process.",
            "No real_primary_latest pointer was refreshed by the V2 preparation attempt.",
            "No instrument zero/span/coefficient/calibration parameter write occurred.",
        ],
        "prohibited_actions": [
            "Do not switch the default entry to V2.",
            "Do not close or delete V1 as a fallback path.",
            "Do not run real compare/real verify from this worksheet.",
            "Do not open real COM/serial or PLC/valve/instrument/PACE control from this batch.",
            "Do not refresh real_primary_latest.",
        ],
        "evidence_source": "offline_worksheet",
        "not_real_acceptance_evidence": True,
    }


def build_cutover_candidate_worksheet(
    *,
    changed_paths: Iterable[str | Path] = (),
    ignored_paths: Iterable[str | Path] | None = None,
    third_batch_cloud_commit: str = "cba08beb",
    third_batch_scope: str = "narrowed_skip0_co2_only",
    third_batch_conclusion: str = "replacement-validation path usable",
    third_batch_cutover_ready: bool = False,
    generated_at: str | None = None,
) -> dict[str, object]:
    freeze_check = build_v1_freeze_check(changed_paths, ignored_paths=ignored_paths)
    rollback_guard = build_rollback_guard()
    readiness_items = [dict(seed.__dict__) for seed in READINESS_SEEDS]
    readiness_items.append(
        {
            "key": "v1_remains_frozen",
            "label": "V1 remains frozen",
            "status": str(freeze_check["status"]),
            "evidence_path": FREEZE_CHECK_SUMMARY_FILENAME,
            "reason": str(freeze_check["reason"]),
            "remaining_blocker": str(freeze_check["remaining_blocker"]),
            "blocks_dry_run_preparation": not bool(freeze_check["v1_remains_frozen"]),
        }
    )
    _validate_readiness_items(readiness_items)
    selected_conclusion = _select_conclusion(readiness_items)
    status_counts = {
        status: sum(1 for item in readiness_items if item["status"] == status)
        for status in (STATUS_GREEN, STATUS_YELLOW, STATUS_RED)
    }
    return {
        "schema_version": WORKSHEET_SCHEMA_VERSION,
        "artifact_type": WORKSHEET_ARTIFACT_TYPE,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stage": "step2_real_machine_dry_run_preparation",
        "third_batch": {
            "cloud_closure_commit": third_batch_cloud_commit,
            "scope": third_batch_scope,
            "conclusion": third_batch_conclusion,
            "cutover_ready": bool(third_batch_cutover_ready),
        },
        "boundary": {
            "default_entry_remains_v1": True,
            "v2_replaces_v1": False,
            "cutover_ready": False,
            "real_write_allowed": False,
            "real_com_serial_allowed": False,
            "real_acceptance_allowed": False,
            "real_primary_latest_refresh_allowed": False,
            "simulated_latest_can_replace_real_latest": False,
            "not_real_acceptance_evidence": True,
            "evidence_source": "simulated",
        },
        "allowed_conclusions": dict(CONCLUSION_LABELS),
        "selected_conclusion": selected_conclusion,
        "selected_conclusion_label": CONCLUSION_LABELS[selected_conclusion],
        "status_counts": status_counts,
        "readiness_items": readiness_items,
        "rollback_guard": rollback_guard,
        "freeze_check": freeze_check,
    }


def _validate_readiness_items(items: Sequence[dict[str, object]]) -> None:
    required = {"status", "evidence_path", "reason", "remaining_blocker"}
    for item in items:
        missing = sorted(required - set(item))
        if missing:
            raise ValueError(f"readiness item {item.get('key') or '<unknown>'} missing fields: {missing}")
        status = str(item.get("status") or "")
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid readiness status for {item.get('key')}: {status}")


def _select_conclusion(items: Sequence[dict[str, object]]) -> str:
    red_items = [item for item in items if item.get("status") == STATUS_RED]
    if red_items:
        return CONCLUSION_P0_BLOCKED
    blocking_yellow = [
        item
        for item in items
        if item.get("status") == STATUS_YELLOW and bool(item.get("blocks_dry_run_preparation"))
    ]
    if blocking_yellow:
        return CONCLUSION_P1_FIRST
    return CONCLUSION_DRY_RUN_PREP_ALLOWED


def render_worksheet_markdown(payload: dict[str, object]) -> str:
    rows = list(payload.get("readiness_items") or [])
    lines = [
        "# V2 Cutover Candidate Worksheet",
        "",
        f"Selected conclusion: {payload.get('selected_conclusion_label')}",
        "",
        "Boundary:",
        "- This worksheet supports V2 real-machine dry-run preparation only.",
        "- Real write, real acceptance, default-entry switch, and real_primary_latest refresh remain prohibited.",
        "- V2 is not declared a formal replacement for V1.",
        "",
        "| Item | Status | Evidence | Reason | Remaining blocker |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {label} | {status} | {evidence} | {reason} | {blocker} |".format(
                label=_md_cell(row.get("label") or row.get("key")),
                status=_md_cell(row.get("status")),
                evidence=_md_cell(row.get("evidence_path")),
                reason=_md_cell(row.get("reason")),
                blocker=_md_cell(row.get("remaining_blocker")),
            )
        )
    lines.extend(
        [
            "",
            "Status semantics:",
            "- green: sufficient for this dry-run-preparation worksheet.",
            "- yellow: reviewer attention or future dry-run evidence remains, but it is not a P0 blocker here.",
            "- red: P0 blocker for entering V2 real-machine dry-run preparation.",
            "",
        ]
    )
    return "\n".join(lines)


def render_rollback_guard_markdown(payload: dict[str, object]) -> str:
    guard = dict(payload.get("rollback_guard") or payload)
    lines = [
        "# V2 Rollback And Fallback Guard",
        "",
        "This guard is a worksheet/SOP only. It does not switch the default entry and does not execute rollback.",
        "",
        "## Default Entry",
        f"- Status: {dict(guard.get('default_entry') or {}).get('status')}",
        f"- Path: {dict(guard.get('default_entry') or {}).get('path')}",
        "- V1 remains the production default and rollback reference.",
        "",
        "## Future Dry-Run Boundary",
    ]
    boundary = dict(guard.get("future_dry_run_boundary") or {})
    for key, value in boundary.items():
        lines.append(f"- {key}: {str(value).lower()}")
    for title, key in (
        ("Preserve V1 Baselines", "preserve_v1_baselines"),
        ("Rollback-Sensitive Files", "rollback_sensitive_files"),
        ("Rollback Triggers", "rollback_triggers"),
        ("Rollback Steps", "rollback_steps"),
        ("Post-Rollback Verification", "post_rollback_verification"),
        ("Prohibited Actions", "prohibited_actions"),
    ):
        lines.extend(["", f"## {title}"])
        for item in list(guard.get(key) or []):
            lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def render_freeze_check_markdown(freeze_check: dict[str, object]) -> str:
    lines = [
        "# V1 Freeze Check Summary",
        "",
        f"Status: {freeze_check.get('status')}",
        f"Reason: {freeze_check.get('reason')}",
        "",
        "Forbidden changed paths:",
    ]
    forbidden = list(freeze_check.get("forbidden_changed_paths") or [])
    if forbidden:
        lines.extend(f"- {item}" for item in forbidden)
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def write_cutover_candidate_artifacts(
    output_dir: str | Path,
    payload: dict[str, object],
) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    worksheet_path = target / CUTOVER_WORKSHEET_FILENAME
    worksheet_md_path = target / CUTOVER_WORKSHEET_MARKDOWN_FILENAME
    rollback_path = target / ROLLBACK_GUARD_FILENAME
    freeze_path = target / FREEZE_CHECK_SUMMARY_FILENAME
    worksheet_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    worksheet_md_path.write_text(render_worksheet_markdown(payload), encoding="utf-8")
    rollback_path.write_text(render_rollback_guard_markdown(payload), encoding="utf-8")
    freeze_path.write_text(
        json.dumps(dict(payload.get("freeze_check") or {}), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "worksheet_json": str(worksheet_path),
        "worksheet_markdown": str(worksheet_md_path),
        "rollback_guard": str(rollback_path),
        "freeze_check_summary": str(freeze_path),
    }


def collect_git_changed_paths(
    repo_root: str | Path,
    *,
    base_ref: str = "HEAD",
    include_untracked: bool = True,
    ignored_untracked_prefixes: Iterable[str] = DEFAULT_IGNORED_UNTRACKED_PREFIXES,
) -> tuple[list[str], list[str]]:
    root = Path(repo_root)
    changed: set[str] = set()
    ignored: set[str] = set()
    for args in (
        ["diff", "--name-only", base_ref, "--"],
        ["diff", "--cached", "--name-only", "--"],
    ):
        changed.update(_run_git_lines(root, args))
    if include_untracked:
        ignored_prefixes = tuple(normalize_repo_path(prefix) for prefix in ignored_untracked_prefixes)
        for path in _run_git_lines(root, ["ls-files", "--others", "--exclude-standard"]):
            normalized = normalize_repo_path(path)
            if any(normalized.startswith(prefix) for prefix in ignored_prefixes):
                ignored.add(normalized)
            else:
                changed.add(normalized)
    return sorted(changed), sorted(ignored)


def _run_git_lines(repo_root: Path, args: Sequence[str]) -> list[str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def _md_cell(value: object) -> str:
    text = str(value if value is not None else "").replace("\n", " ").strip()
    return text.replace("|", "\\|")
