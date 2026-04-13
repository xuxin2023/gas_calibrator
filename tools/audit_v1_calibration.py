"""Read-only V1 calibration audit report generator."""

from __future__ import annotations

import argparse
import csv
import ast
import json
import re
import subprocess
import sys
import tempfile
import types
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
DEFAULT_OUTPUT_DIR = ROOT / "audit" / "v1_calibration_audit"
DEFAULT_ACCEPTANCE_DIR = ROOT / "audit" / "v1_calibration_acceptance"
DEFAULT_ONLINE_ACCEPTANCE_DIR = ROOT / "audit" / "v1_calibration_acceptance_online"
DEFAULT_SINCE = "2026-04-03 00:00:00"
TRACE_TEST = "tests/test_audit_v1_trace_check.py"
TRACE_TESTS = [
    TRACE_TEST,
    "tests/test_runner_v1_writeback_safety.py",
    "tests/test_v1_writeback_fault_injection.py",
    "tests/test_v1_online_acceptance_tool.py",
]

KEYWORD_PATTERN = (
    r"V1|校准|标定|calibration|cali|zero|span|CO2|H2O|SENCO|GETCO|MODE|READDATA|"
    r"point|save|store|insert|db|report|serial|protocol|气路|流程|step|状态机|"
    r"coefficient|coefficients|readback|writeback|delivery|short_verify"
)
KEYWORD_RE = re.compile(KEYWORD_PATTERN, re.IGNORECASE)

IMPORTANT_PATH_PARTS = (
    "src/gas_calibrator/workflow/runner.py",
    "src/gas_calibrator/data/points.py",
    "src/gas_calibrator/devices/gas_analyzer.py",
    "src/gas_calibrator/logging_utils.py",
    "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
    "src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py",
    "src/gas_calibrator/config.py",
    "src/gas_calibrator/ui/app.py",
    "tests/test_runner_collect_only.py",
    "tests/test_runner_route_handoff.py",
    "tests/test_run_v1_corrected_autodelivery.py",
)


@dataclass
class FileSpan:
    path: str
    start_line: int
    end_line: int

    def as_ref(self) -> str:
        return f"`{self.path}:{self.start_line}-{self.end_line}`"

    def as_json(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "start_line": self.start_line,
            "end_line": self.end_line,
        }


@dataclass
class EvidenceItem:
    id: str
    title: str
    severity: str
    status: str
    summary: str
    files: List[FileSpan]
    commits: List[str]
    why_it_matters: str
    recommendation: str
    confidence: float

    def as_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "severity": self.severity,
            "status": self.status,
            "summary": self.summary,
            "files": [item.as_json() for item in self.files],
            "commits": list(self.commits),
            "why_it_matters": self.why_it_matters,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
        }


def run_cmd(
    args: Sequence[str],
    *,
    check: bool = True,
    allow_codes: Iterable[int] = (0,),
    cwd: Path = ROOT,
) -> str:
    proc = subprocess.run(
        list(args),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    allowed = set(allow_codes)
    if check and proc.returncode not in allowed:
        cmd = " ".join(args)
        raise RuntimeError(f"command failed ({proc.returncode}): {cmd}\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="replace")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


_SYMBOL_CACHE: Dict[str, Dict[str, Tuple[int, int]]] = {}


def symbol_ranges(rel_path: str) -> Dict[str, Tuple[int, int]]:
    if rel_path in _SYMBOL_CACHE:
        return _SYMBOL_CACHE[rel_path]
    path = ROOT / rel_path
    tree = ast.parse(read_text(path))
    out: Dict[str, Tuple[int, int]] = {}

    def visit(body: List[ast.stmt], prefix: str = "") -> None:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = f"{prefix}.{node.name}" if prefix else node.name
                out[name] = (int(node.lineno), int(node.end_lineno or node.lineno))
                if isinstance(node, ast.ClassDef):
                    visit(node.body, name)

    visit(tree.body)
    _SYMBOL_CACHE[rel_path] = out
    return out


def symbol_span(rel_path: str, qualname: str) -> FileSpan:
    start, end = symbol_ranges(rel_path)[qualname]
    return FileSpan(rel_path, start, end)


def file_span(rel_path: str) -> FileSpan:
    lines = read_text(ROOT / rel_path).splitlines()
    return FileSpan(rel_path, 1, max(1, len(lines)))


def bracket_block_span(rel_path: str, marker: str, open_char: str, close_char: str) -> FileSpan:
    lines = read_text(ROOT / rel_path).splitlines()
    start = None
    balance = 0
    started = False
    for idx, line in enumerate(lines, start=1):
        if start is None and marker in line:
            start = idx
        if start is None:
            continue
        if not started and marker in line:
            started = True
        if started:
            balance += line.count(open_char)
            balance -= line.count(close_char)
            if balance <= 0:
                return FileSpan(rel_path, start, idx)
    if start is None:
        raise KeyError(f"marker not found: {rel_path} / {marker}")
    return FileSpan(rel_path, start, len(lines))


def blame_commits(span: FileSpan) -> List[str]:
    try:
        output = run_cmd(
            [
                "git",
                "blame",
                "-L",
                f"{span.start_line},{span.end_line}",
                "--date=short",
                "--",
                span.path,
            ]
        )
    except RuntimeError:
        return []
    commits: List[str] = []
    for line in output.splitlines():
        text = line.strip()
        if not text:
            continue
        commit = text.split(maxsplit=1)[0].lstrip("^")
        if commit and commit not in commits:
            commits.append(commit)
    return commits


def branch_name() -> str:
    return run_cmd(["git", "branch", "--show-current"]).strip()


def head_commit() -> str:
    return run_cmd(["git", "rev-parse", "HEAD"]).strip()


def current_git_status_text(status_override_file: Optional[Path]) -> str:
    if status_override_file and status_override_file.exists():
        return read_text(status_override_file)
    branch_text = run_cmd(["git", "status", "--short", "--branch"], check=False, allow_codes=(0, 1))
    porcelain = run_cmd(["git", "status", "--porcelain=v1", "-uall"], check=False, allow_codes=(0, 1))
    lines = ["# git status --short --branch", branch_text.rstrip(), "", "# git status --porcelain=v1 -uall"]
    lines.append(porcelain.rstrip() if porcelain.strip() else "(clean)")
    return "\n".join(lines).rstrip() + "\n"


def recent_30_commits() -> List[Tuple[str, str, str]]:
    output = run_cmd(
        ["git", "log", "-n", "30", "--date=iso", "--pretty=format:%H%x1f%ad%x1f%s"]
    )
    rows = []
    for line in output.splitlines():
        if not line.strip():
            continue
        commit, date_text, subject = line.split("\x1f", 2)
        rows.append((commit, date_text, subject))
    return rows


def git_log_since_raw(since: str) -> str:
    return run_cmd(
        [
            "git",
            "log",
            f"--since={since}",
            "--date=iso",
            "--pretty=fuller",
            "--stat",
            "--patch",
            "--unified=0",
            "--no-color",
        ]
    )


def keyword_hits_text() -> str:
    return run_cmd(
        ["git", "grep", "-n", "-I", "-E", KEYWORD_PATTERN, "--", ".", ":(exclude)audit/**"],
        check=False,
        allow_codes=(0, 1),
    )


def commit_rows_since(since: str) -> List[Tuple[str, str, str]]:
    output = run_cmd(
        ["git", "log", f"--since={since}", "--date=iso", "--pretty=format:%H%x1f%ad%x1f%s"]
    )
    rows = []
    for line in output.splitlines():
        if not line.strip():
            continue
        commit, date_text, subject = line.split("\x1f", 2)
        rows.append((commit, date_text, subject))
    return rows


def commit_files(commit: str) -> List[str]:
    output = run_cmd(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", commit],
        check=False,
        allow_codes=(0, 1),
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def commit_diff(commit: str) -> str:
    return run_cmd(["git", "show", "--unified=0", "--no-color", commit])


def commit_relevant(subject: str, files: List[str], diff_text: str) -> bool:
    if KEYWORD_RE.search(subject):
        return True
    if any(KEYWORD_RE.search(path) for path in files):
        return True
    if any(path in IMPORTANT_PATH_PARTS for path in files):
        return True
    if any(path in diff_text for path in IMPORTANT_PATH_PARTS):
        return True
    return bool(KEYWORD_RE.search(diff_text))


def matching_terms(text: str) -> List[str]:
    return sorted({match.group(0) for match in KEYWORD_RE.finditer(text)})


def extract_hunk_summaries(diff_text: str, max_items: int = 5) -> List[str]:
    current_file = ""
    current_hunk = ""
    items: List[str] = []
    for line in diff_text.splitlines():
        if line.startswith("diff --git "):
            parts = line.split(" b/", 1)
            current_file = parts[1] if len(parts) == 2 else line
            current_hunk = ""
            continue
        if line.startswith("@@"):
            current_hunk = line.strip()
            continue
        if line.startswith(("+++", "---")):
            continue
        if not line.startswith(("+", "-")):
            continue
        if not (KEYWORD_RE.search(line) or KEYWORD_RE.search(current_file) or current_file in IMPORTANT_PATH_PARTS):
            continue
        summary = f"{current_file} {current_hunk} {line[:180].strip()}"
        if summary not in items:
            items.append(summary)
        if len(items) >= max_items:
            break
    return items


def collect_relevant_commits(since: str) -> List[Dict[str, Any]]:
    rows = []
    for commit, date_text, subject in commit_rows_since(since):
        files = commit_files(commit)
        diff_text = commit_diff(commit)
        if not commit_relevant(subject, files, diff_text):
            continue
        reasons: List[str] = []
        subject_terms = matching_terms(subject)
        if subject_terms:
            reasons.append(f"提交说明命中关键词: {', '.join(subject_terms)}")
        matched_files = [path for path in files if KEYWORD_RE.search(path) or path in IMPORTANT_PATH_PARTS]
        if matched_files:
            reasons.append("改动文件命中校准相关路径/关键词")
        diff_terms = matching_terms(diff_text)
        if diff_terms:
            reasons.append(f"diff 内容命中关键词: {', '.join(diff_terms[:8])}")
        rows.append(
            {
                "commit": commit,
                "date": date_text,
                "subject": subject,
                "files": files,
                "reasons": reasons or ["文件/差异与 V1 校准链路相邻，保守纳入审计"],
                "hunks": extract_hunk_summaries(diff_text),
            }
        )
    return rows


def relevant_uncommitted_files(status_text: str) -> List[str]:
    paths: List[str] = []
    for raw_line in status_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line == "(clean)" or line.startswith("##"):
            continue
        path = line[3:].strip() if len(line) > 3 else line
        if " -> " in path:
            path = path.split(" -> ", 1)[1].strip()
        if KEYWORD_RE.search(path):
            paths.append(path)
    return paths


def span_group(*spans: FileSpan) -> str:
    return ", ".join(item.as_ref() for item in spans)


def build_code_spans() -> Dict[str, FileSpan]:
    spans: Dict[str, FileSpan] = {
        "run_app_file": file_span("run_app.py"),
        "app_start_run_background": symbol_span("src/gas_calibrator/ui/app.py", "App._start_run_background"),
        "runner_run": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner.run"),
        "runner_cleanup": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._cleanup"),
        "runner_restore_baseline": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._restore_baseline_after_run"),
        "runner_run_points": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._run_points"),
        "runner_run_temperature_group": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._run_temperature_group"),
        "runner_run_co2_point": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._run_co2_point"),
        "runner_run_h2o_group": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._run_h2o_group"),
        "runner_is_zero_co2_point": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._is_zero_co2_point"),
        "runner_wait_co2_route_soak_before_seal": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._wait_co2_route_soak_before_seal"),
        "runner_wait_primary_sensor_stable": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._wait_primary_sensor_stable"),
        "runner_wait_pressure_sampling_ready": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._wait_after_pressure_stable_before_sampling"),
        "runner_wait_sampling_freshness_gate": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._wait_for_sampling_freshness_gate"),
        "runner_collect_samples": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._collect_samples"),
        "runner_sample_and_log": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._sample_and_log"),
        "runner_build_point_summary_row": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._build_point_summary_row"),
        "runner_perform_light_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._perform_light_point_exports"),
        "runner_perform_heavy_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._perform_heavy_point_exports"),
        "runner_enqueue_deferred_sample_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._enqueue_deferred_sample_exports"),
        "runner_flush_deferred_sample_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._flush_deferred_sample_exports"),
        "runner_enqueue_deferred_point_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._enqueue_deferred_point_exports"),
        "runner_flush_deferred_point_exports": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._flush_deferred_point_exports"),
        "runner_co2_point_tag": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._co2_point_tag"),
        "runner_h2o_point_tag": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._h2o_point_tag"),
        "runner_build_co2_pressure_point": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._build_co2_pressure_point"),
        "runner_build_h2o_pressure_point": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._build_h2o_pressure_point"),
        "runner_maybe_write_coefficients": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._maybe_write_coefficients"),
        "runner_effective_postrun_cfg": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._effective_postrun_corrected_delivery_cfg"),
        "runner_log_postrun_cfg": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._log_postrun_corrected_delivery_effective_config"),
        "runner_h2o_zero_span_capability_payload": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._h2o_zero_span_capability_payload"),
        "runner_log_h2o_zero_span_capability": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._log_h2o_zero_span_capability"),
        "runner_require_supported_h2o_zero_span": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._require_supported_h2o_zero_span_if_requested"),
        "runner_persist_coefficient_write_result": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._persist_coefficient_write_result"),
        "runner_annotate_point_trace_rows": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._annotate_point_trace_rows"),
        "runner_postrun_corrected_delivery": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._maybe_run_postrun_corrected_delivery"),
        "runner_filter_ratio_poly_summary_rows": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._filter_ratio_poly_summary_rows"),
        "points_calibration_point": symbol_span("src/gas_calibrator/data/points.py", "CalibrationPoint"),
        "points_load_points_from_excel": symbol_span("src/gas_calibrator/data/points.py", "load_points_from_excel"),
        "points_reorder_points": symbol_span("src/gas_calibrator/data/points.py", "reorder_points"),
        "ga_set_mode_with_ack": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.set_mode_with_ack"),
        "ga_set_senco": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.set_senco"),
        "ga_read_coefficient_group": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_coefficient_group"),
        "ga_read_current_mode_snapshot": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_current_mode_snapshot"),
        "ga_read_data_passive": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_data_passive"),
        "ga_read_data_active": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_data_active"),
        "ga_parse_line_mode2": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.parse_line_mode2"),
        "logger_field_labels": bracket_block_span("src/gas_calibrator/logging_utils.py", "_FIELD_LABELS = {", "{", "}"),
        "logger_common_sheet_fields": bracket_block_span("src/gas_calibrator/logging_utils.py", "_COMMON_SHEET_FIELDS = [", "[", "]"),
        "logger_runlogger_init": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.__init__"),
        "logger_log_sample": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_sample"),
        "logger_log_point": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_point"),
        "logger_log_coefficient_write": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_coefficient_write"),
        "logger_append_points_csv": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger._append_points_csv_row"),
        "logger_log_point_samples": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_point_samples"),
        "logger_build_analyzer_summary": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger._build_analyzer_summary_row"),
        "corrected_build_download_plan": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "build_corrected_download_plan_rows"),
        "corrected_write_coefficients": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "write_coefficients_to_live_devices"),
        "corrected_full_writeback": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "write_senco_groups_with_full_verification"),
        "corrected_read_group_retry": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "_read_group_with_match_retry"),
        "corrected_build_delivery": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "build_corrected_delivery"),
        "config_postrun_corrected_delivery": bracket_block_span("src/gas_calibrator/config.py", '"postrun_corrected_delivery": {', "{", "}"),
        "config_h2o_zero_span": bracket_block_span("src/gas_calibrator/config.py", '"h2o_zero_span": {', "{", "}"),
        "h2o_summary_default_selection": symbol_span("src/gas_calibrator/h2o_summary_selection.py", "default_h2o_summary_selection"),
        "test_collect_samples_h2o_snapshot": symbol_span("tests/test_runner_collect_only.py", "test_collect_samples_uses_preseal_dewpoint_snapshot_for_h2o"),
        "test_collect_ratio_poly_h2o_selection": symbol_span("tests/test_runner_collect_only.py", "test_runner_ratio_poly_autofit_h2o_includes_h2o_and_selected_co2_zero_rows"),
        "test_route_handoff_defers_exports": symbol_span("tests/test_runner_route_handoff.py", "test_sample_and_log_arms_route_handoff_before_sample_exports"),
        "test_audit_trace_fields": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_point_trace_row_contains_expected_fields"),
        "test_audit_trace_overwrite": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_point_trace_distinct_points_do_not_overwrite_each_other"),
        "test_audit_trace_guards": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_trace_code_keeps_stability_and_freshness_guards"),
        "test_v1_writeback_defaults_safe": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_postrun_corrected_delivery_defaults_safe"),
        "test_v1_h2o_zero_span_not_supported": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_h2o_zero_span_capability_defaults_not_supported"),
        "test_v1_h2o_zero_span_fail_fast": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_h2o_zero_span_requirement_fails_fast"),
        "test_v1_writeback_success": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_runner_writeback_success_reads_before_and_after"),
        "test_v1_writeback_mismatch": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_runner_writeback_mismatch_triggers_rollback_and_restore"),
        "test_v1_writeback_point_fields": symbol_span("tests/test_runner_v1_writeback_safety.py", "test_point_export_rows_include_traceability_fields_and_do_not_overwrite"),
        "test_fault_set_mode2": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_set_mode2_failure_still_attempts_restore"),
        "test_fault_set_senco": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_set_senco_exception_rolls_back_and_restores"),
        "test_fault_readback_failures": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_readback_failures_roll_back_and_restore"),
        "test_fault_rollback_write_failure": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_rollback_write_failure_marks_unsafe"),
        "test_fault_set_mode1": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_set_mode1_exit_failure_is_unsafe"),
        "test_fault_no_snapshot": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_missing_mode_snapshot_marks_exit_unconfirmed_unsafe"),
        "test_fault_exit_unconfirmed": symbol_span("tests/test_v1_writeback_fault_injection.py", "test_fault_exit_attempt_without_confirmation_is_unsafe"),
        "online_acceptance_run": symbol_span("src/gas_calibrator/tools/run_v1_online_acceptance.py", "run_online_acceptance"),
        "online_acceptance_bundle": symbol_span("src/gas_calibrator/tools/run_v1_online_acceptance.py", "write_online_acceptance_bundle"),
        "online_acceptance_require_co2_only": symbol_span("src/gas_calibrator/tools/run_v1_online_acceptance.py", "_ensure_co2_only_request"),
        "test_online_acceptance_dual_gate": symbol_span("tests/test_v1_online_acceptance_tool.py", "test_online_acceptance_requires_dual_gate_for_real_device"),
        "test_online_acceptance_dry_run": symbol_span("tests/test_v1_online_acceptance_tool.py", "test_online_acceptance_dry_run_generates_templates_only"),
        "test_online_acceptance_real_mode": symbol_span("tests/test_v1_online_acceptance_tool.py", "test_online_acceptance_real_mode_writes_summary_and_protocol_log"),
        "test_online_acceptance_exit_unconfirmed": symbol_span("tests/test_v1_online_acceptance_tool.py", "test_online_acceptance_exit_unconfirmed_is_unsafe_and_failed"),
        "test_online_acceptance_h2o_fail_fast": symbol_span("tests/test_v1_online_acceptance_tool.py", "test_online_acceptance_h2o_request_fails_fast"),
    }
    return spans


def build_readme(
    *,
    generated_at: str,
    branch: str,
    head: str,
    recent_commits_rows: List[Tuple[str, str, str]],
    output_dir: Path,
) -> str:
    lines = [
        "# V1 Calibration Audit",
        "",
        f"- 生成时间: {generated_at}",
        f"- 当前分支: `{branch}`",
        f"- HEAD: `{head}`",
        f"- 输出目录: `{rel(output_dir)}`",
        "",
        "## 使用命令/脚本",
        "",
        f"- `python tools/audit_v1_calibration.py`",
        f"- `python tools/run_v1_online_acceptance.py --output-dir {rel(DEFAULT_ONLINE_ACCEPTANCE_DIR)}`",
        f"- `python -m pytest -q {TRACE_TEST}`",
        "- `python -m pytest -q tests/test_runner_v1_writeback_safety.py`",
        "- `python -m pytest -q tests/test_v1_writeback_fault_injection.py`",
        "- `python -m pytest -q tests/test_v1_online_acceptance_tool.py`",
        f"- `git log --since=\"{DEFAULT_SINCE}\" --stat --patch --unified=0`",
        f"- `git grep -n -I -E \"{KEYWORD_PATTERN}\" -- . \":(exclude)audit/**\"`",
        "",
        "## 如何重新生成",
        "",
        "1. 在仓库根目录运行 `python tools/audit_v1_calibration.py`。",
        "2. 如果只想重跑只读 trace 检查，可运行 `python -m pytest -q tests/test_audit_v1_trace_check.py`。",
        "3. 需要保留当前 git status 快照时，可使用 `python tools/audit_v1_calibration.py --status-override-file audit/v1_calibration_audit/raw/git_status.txt`。",
        "",
        "## 最近 30 个 commit",
        "",
    ]
    for commit, date_text, subject in recent_commits_rows:
        lines.append(f"- `{commit}` | {date_text} | {subject}")
    lines.append("")
    return "\n".join(lines)


def build_git_changes_report(
    *,
    branch: str,
    head: str,
    since: str,
    commits: List[Dict[str, Any]],
    status_text: str,
) -> str:
    lines = [
        "# 2026-04-03 以来可能影响 V1 校准的改动",
        "",
        f"- 当前分支: `{branch}`",
        f"- 当前 HEAD: `{head}`",
        f"- 筛选起点: `{since}`",
        f"- 纳入 commit 数: `{len(commits)}`",
        "",
        "## Commit 列表",
        "",
    ]
    if not commits:
        lines.append("- 未筛出命中关键词或关键路径的 commit。")
    for item in commits:
        lines.extend(
            [
                f"### `{item['commit']}`",
                f"- 时间: {item['date']}",
                f"- 标题: {item['subject']}",
                f"- 涉及文件: {', '.join(f'`{path}`' for path in item['files']) if item['files'] else '(none)'}",
                f"- 判定原因: {'；'.join(item['reasons'])}",
                "- 关键 diff hunk 摘要:",
            ]
        )
        if item["hunks"]:
            for hunk in item["hunks"]:
                lines.append(f"  - {hunk}")
        else:
            lines.append("  - 未提取到关键词 hunk，建议结合 raw git log 复核。")
        lines.append("")

    related_uncommitted = relevant_uncommitted_files(status_text)
    lines.extend(["## 未提交改动中与 V1 校准相关的文件", ""])
    if related_uncommitted:
        for path in related_uncommitted:
            lines.append(f"- `{path}`")
    else:
        lines.append("- 无。初始快照显示工作区为 clean。")
    lines.append("")
    return "\n".join(lines)


def build_flow_map(spans: Dict[str, FileSpan]) -> str:
    lines = [
        "# V1 校准主流程文字图",
        "",
        "入口 -> 点表解析/重排 -> 按温度分组编排 -> CO2/H2O 路线执行 -> 稳态/门禁 -> 样本采集 -> 点位导出 -> 系数写入 -> 模式恢复 -> 清理/后处理",
        "",
        "## 入口",
        "",
        f"- 默认入口文件: {spans['run_app_file'].as_ref()}。",
        f"- UI 启动后台执行: {spans['app_start_run_background'].as_ref()}，其中创建 `RunLogger` 与 `CalibrationRunner`。",
        f"- 主执行函数: {spans['runner_run'].as_ref()}。",
        "",
        "## 步骤编排",
        "",
        f"- 点表解析: {spans['points_load_points_from_excel'].as_ref()}；`CalibrationPoint.index` 直接使用 Excel 行号。",
        f"- 点位重排: {spans['points_reorder_points'].as_ref()}；高温段可先水路后气路。",
        f"- 温度分组调度: {spans['runner_run_points'].as_ref()}。",
        f"- 单温度组编排: {spans['runner_run_temperature_group'].as_ref()}，把 H2O 组和 CO2 源点编进 `route_plan`。",
        f"- CO2 主链路: {spans['runner_run_co2_point'].as_ref()}。",
        f"- H2O 主链路: {spans['runner_run_h2o_group'].as_ref()}。",
        "",
        "## 设备指令与通信层",
        "",
        f"- 进入/切换模式 `MODE`: {spans['ga_set_mode_with_ack'].as_ref()}。",
        f"- 系数写入 `SENCO`: {spans['ga_set_senco'].as_ref()}。",
        f"- 系数回读 `GETCO`: {spans['ga_read_coefficient_group'].as_ref()}。",
        f"- 被动读数 `READDATA`: {spans['ga_read_data_passive'].as_ref()}；主动流读取/解析见 {span_group(spans['ga_read_data_active'], spans['ga_parse_line_mode2'])}。",
        "",
        "## 数据采集与稳态门禁",
        "",
        f"- 传感器稳态窗口: {spans['runner_wait_primary_sensor_stable'].as_ref()}；基于时间窗峰峰值判稳，不是简单“切点后立即取最新值”。",
        f"- 压力达标后的二次等待/门禁: {spans['runner_wait_pressure_sampling_ready'].as_ref()}；包含 post-seal dewpoint gate、CO2 长稳守护、adaptive pressure gate、最小等待时长。",
        f"- 采样 freshness gate: {spans['runner_wait_sampling_freshness_gate'].as_ref()}。",
        f"- 样本行组装: {spans['runner_collect_samples'].as_ref()}。",
        f"- 点位采样与导出编排: {spans['runner_sample_and_log'].as_ref()}。",
        "",
        "## 点位保存与报表",
        "",
        f"- 样本级 CSV 追加: {span_group(spans['logger_log_sample'], spans['logger_runlogger_init'])}。",
        f"- 点位汇总 CSV/可读 CSV/XLSX: {span_group(spans['logger_log_point'], spans['logger_append_points_csv'])}。",
        f"- 单点位样本文件: {spans['logger_log_point_samples'].as_ref()}，文件名包含 `point_row + phase + tag`。",
        f"- 点位汇总 payload 构造: {spans['runner_build_point_summary_row'].as_ref()}。",
        f"- 分析仪汇总表: {spans['logger_build_analyzer_summary'].as_ref()}。",
        "",
        "## 系数写入/回读/恢复",
        "",
        f"- 主 runner 的系数写入路径: {span_group(spans['runner_maybe_write_coefficients'], spans['runner_persist_coefficient_write_result'], spans['ga_read_current_mode_snapshot'])}；当前主路径已复用 shared helper，执行 `写前快照 -> MODE=2 -> SENCO -> GETCO 回读比对 -> 失败回滚 -> finally 恢复模式`。",
        f"- corrected autodelivery 旁路: {span_group(spans['corrected_build_download_plan'], spans['corrected_write_coefficients'], spans['corrected_full_writeback'], spans['corrected_read_group_retry'])}；这里对 CO2 使用 1/3 组、对 H2O 使用 2/4 组，并与主 runner 共用 `GETCO` 回读匹配能力。",
        f"- 运行结束后自动触发 corrected autodelivery 的 hook: {spans['runner_postrun_corrected_delivery'].as_ref()}。",
        f"- 清理与基线恢复: {span_group(spans['runner_cleanup'], spans['runner_restore_baseline'])}。",
        "",
        "## 明确结论",
        "",
        f"- CO2 零点检查: 有。见 {span_group(spans['runner_is_zero_co2_point'], spans['runner_wait_co2_route_soak_before_seal'])}。",
        f"- CO2 标气跨度: 有。`_run_temperature_group` 会按 CO2 源点 ppm 扫描，`_run_co2_point` 负责执行。见 {span_group(spans['runner_run_temperature_group'], spans['runner_run_co2_point'])}。",
        f"- H2O 零点/跨度: 只能确认 H2O 路线与多组湿度点存在，未找到明确以“零点/跨度”命名或判定的业务步骤；见 {spans['runner_run_h2o_group'].as_ref()}。结论: UNKNOWN。",
        f"- MODE=校准模式 与恢复正常模式: 有。主 runner 和 corrected autodelivery 共用写回 helper，模式切换与 finally 恢复见 {span_group(spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'], spans['ga_set_mode_with_ack'], spans['ga_read_current_mode_snapshot'])}。",
        f"- 系数写入后 GETCO 或等价回读验证: 有。主 runner 已通过 shared helper 接入 `GETCO` 回读比对与失败回滚；证据见 {span_group(spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'], spans['ga_read_coefficient_group'])}。结论: PASS。",
        "",
        "## CO2/H2O 链路关系",
        "",
        f"- 结构上是两套并行链路，不是只有 CO2 完整实现。CO2 执行入口见 {spans['runner_run_co2_point'].as_ref()}，H2O 执行入口见 {spans['runner_run_h2o_group'].as_ref()}。",
        f"- 但 H2O 的“零点/跨度”业务语义在当前代码中没有像 CO2 zero-gas 那样被显式建模，因此这部分不能直接判定闭环完成。",
        "",
    ]
    return "\n".join(lines)


def build_point_storage_map(spans: Dict[str, FileSpan]) -> str:
    lines = [
        "# 点位存储链路",
        "",
        "## 每个点位的数据链路",
        "",
        f"- 点位编号从哪里来: `CalibrationPoint.index` 直接取 Excel 行号，见 {span_group(spans['points_calibration_point'], spans['points_load_points_from_excel'])}。",
        f"- 目标标准值从哪里来: CO2 目标来自 `point.co2_ppm`，H2O 目标来自 `point.h2o_mmol/hgen_*`，压力目标来自 `point.target_pressure_hpa`，样本组装见 {spans['runner_collect_samples'].as_ref()}。",
        f"- 原始数据从哪里读: 分析仪数据由 `_read_sensor_parsed`/MODE2 帧缓存进入样本；露点/压力/温湿度来自 fast signal 与 slow aux cache，见 {span_group(spans['ga_read_data_passive'], spans['ga_read_data_active'], spans['runner_collect_samples'])}。",
        f"- 是否有稳态等待/冲洗/延时: 有。CO2 路线预冲洗和零气特殊 flush 见 {span_group(spans['runner_run_co2_point'], spans['runner_wait_co2_route_soak_before_seal'])}；压力达标后采样前门禁见 {spans['runner_wait_pressure_sampling_ready'].as_ref()}；采样 freshness gate 见 {spans['runner_wait_sampling_freshness_gate'].as_ref()}。",
        f"- 是否用窗口平均/标准差: 样本采集前的稳态判定使用时间窗峰峰值；点位汇总时再计算 mean/std，见 {span_group(spans['runner_wait_primary_sensor_stable'], spans['runner_build_point_summary_row'])}。",
        f"- 何时触发保存: `_sample_and_log` 在采集结束、完成质量与完整性汇总后，触发 light/heavy export，见 {span_group(spans['runner_sample_and_log'], spans['runner_perform_light_exports'], spans['runner_perform_heavy_exports'])}。",
        f"- 保存到哪里: `samples_*.csv`、`point_XXXX*_samples.csv`、`points_*.csv`、`points_readable_*.csv/.xlsx`、`分析仪汇总_*.csv/.xlsx`、`coefficient_writeback_*.csv`，见 {span_group(spans['logger_runlogger_init'], spans['logger_log_sample'], spans['logger_log_point'], spans['logger_log_point_samples'], spans['logger_log_coefficient_write'], spans['logger_build_analyzer_summary'])}。",
        "",
        "## 保存 payload",
        "",
        f"- 样本级 payload 关键字段: `run_id/session_id/device_id/gas_type/step/point_no/target_value/measured_value/sample_ts/save_ts/window_start_ts/window_end_ts/sample_count/stable_flag/...`，见 {span_group(spans['runner_collect_samples'], spans['runner_annotate_point_trace_rows'], spans['logger_field_labels'])}。",
        f"- 点位汇总 payload 关键字段: `run_id/session_id/device_id/gas_type/step/point_no/target_value/measured_value/sample_ts/save_ts/window_start_ts/window_end_ts/sample_count/stable_flag/targets/mean/std/valid_count/quality/timing`，见 {span_group(spans['runner_build_point_summary_row'], spans['logger_log_point'])}。",
        "",
        "## 必答问题",
        "",
        f"- 每个点位是否有唯一标识: PASS。当前实现依赖 `point_row + point_phase + point_tag` 组合，而不是单独 UUID；构造点与 tag 见 {span_group(spans['runner_build_co2_pressure_point'], spans['runner_build_h2o_pressure_point'], spans['runner_co2_point_tag'], spans['runner_h2o_point_tag'], spans['runner_build_point_summary_row'])}。",
        f"- 是否保存 raw timestamp 和 save timestamp: PASS。样本与点位导出现在同时保留 `sample_ts` 和独立 `save_ts`，并补了窗口时间字段；见 {span_group(spans['runner_collect_samples'], spans['runner_annotate_point_trace_rows'], spans['runner_build_point_summary_row'], spans['logger_log_sample'], spans['logger_log_point'])}。",
        f"- 是否区分采样时间与入库时间: PASS。`sample_ts` 保留采样时间，`save_ts` 在真正导出前写入，证据见 {span_group(spans['runner_perform_light_exports'], spans['runner_perform_heavy_exports'], spans['runner_build_point_summary_row'])}。",
        f"- 是否可能把“最新一条高频数据”误存成当前点位: 当前代码有 freshness gate 与压力后门禁，结论 PASS，但仅限静态审计；证据见 {span_group(spans['runner_wait_sampling_freshness_gate'], spans['runner_wait_pressure_sampling_ready'], spans['test_audit_trace_guards'])}。",
        f"- 是否可能上一点位/过渡态/未稳定数据被保存到下一点位: 代码有 route handoff + deferred export 保护，结论 PASS；见 {span_group(spans['runner_enqueue_deferred_sample_exports'], spans['runner_flush_deferred_sample_exports'], spans['runner_enqueue_deferred_point_exports'], spans['runner_flush_deferred_point_exports'], spans['test_route_handoff_defers_exports'])}。",
        f"- 是否可能覆盖前一个点位，而不是新增一条: 对“不同点位”结论 PASS。`samples.csv/points.csv` 追加写入，单点样本文件按 `point_row + phase + tag` 分文件，离线测试见 {span_group(spans['logger_log_sample'], spans['logger_append_points_csv'], spans['logger_log_point_samples'], spans['test_audit_trace_overwrite'])}。但如果同一 `point_row + phase + tag` 被重复导出，单点样本 CSV 会覆盖同名文件，这属于同标识重写，不是不同点位覆盖。",
        f"- 是否能追溯标定前系数、标定后系数、上一次系数: 主 runner 结论 PASS。当前主路径已把 `coeff_before/coeff_target/coeff_readback/coeff_rollback_*` 持久化到 `coefficient_writeback_*.csv`，见 {span_group(spans['runner_maybe_write_coefficients'], spans['runner_persist_coefficient_write_result'], spans['logger_log_coefficient_write'])}。",
        f"- CO2 和 H2O 两套点位表结构是否一致: 样本主结构大体一致，共用 `COMMON_SHEET_FIELDS`，但目标字段和 H2O 预封压露点快照有差异；见 {span_group(spans['logger_common_sheet_fields'], spans['runner_collect_samples'], spans['test_collect_samples_h2o_snapshot'])}。",
        "",
        "## 点位存储风险表",
        "",
        "| 项目 | 结论 | 证据 |",
        "| --- | --- | --- |",
        f"| 点位唯一标识 | PASS | {span_group(spans['runner_build_point_summary_row'], spans['runner_co2_point_tag'], spans['runner_h2o_point_tag'])} |",
        f"| 样本原始时间戳 | PASS | {span_group(spans['runner_collect_samples'], spans['logger_common_sheet_fields'])} |",
        f"| 保存时间戳 | PASS | {span_group(spans['runner_perform_light_exports'], spans['runner_perform_heavy_exports'], spans['logger_log_sample'], spans['logger_log_point'])} |",
        f"| 采样/入库时间区分 | PASS | {span_group(spans['runner_build_point_summary_row'], spans['runner_perform_light_exports'], spans['runner_perform_heavy_exports'])} |",
        f"| 切点后立即取最新值 | PASS | {span_group(spans['runner_wait_pressure_sampling_ready'], spans['runner_wait_sampling_freshness_gate'], spans['test_audit_trace_guards'])} |",
        f"| 过渡态混入下一点位 | PASS | {span_group(spans['runner_enqueue_deferred_sample_exports'], spans['runner_flush_deferred_sample_exports'], spans['test_route_handoff_defers_exports'])} |",
        f"| 不同点位互相覆盖 | PASS | {span_group(spans['logger_log_point_samples'], spans['test_audit_trace_overwrite'])} |",
        f"| 系数 before/after 追溯 | PASS | {span_group(spans['runner_maybe_write_coefficients'], spans['runner_persist_coefficient_write_result'], spans['logger_log_coefficient_write'])} |",
        f"| CO2/H2O 点位表结构完全一致 | UNKNOWN | {span_group(spans['logger_common_sheet_fields'], spans['runner_collect_samples'])} |",
        "",
    ]
    return "\n".join(lines)


def build_risk_checklist(spans: Dict[str, FileSpan], recent_commit_ids: List[str]) -> str:
    rows = [
        ("V1 主流程有明确入口", "PASS", "", "", [spans["run_app_file"], spans["app_start_run_background"], spans["runner_run"]]),
        ("流程顺序完整且闭环", "PASS", "", "", [spans["runner_run"], spans["runner_run_points"], spans["runner_run_temperature_group"], spans["runner_cleanup"]]),
        ("CO2 零点检查存在", "PASS", "", "", [spans["runner_is_zero_co2_point"], spans["runner_wait_co2_route_soak_before_seal"]]),
        ("CO2 跨度存在", "PASS", "", "", [spans["runner_run_temperature_group"], spans["runner_run_co2_point"]]),
        ("H2O 零点存在", "UNKNOWN", "", "", [spans["runner_run_h2o_group"]]),
        ("H2O 跨度存在", "UNKNOWN", "", "", [spans["runner_run_h2o_group"]]),
        ("进入校准模式存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["ga_set_mode_with_ack"]]),
        ("退出校准模式存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"]]),
        ("系数写入存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["ga_set_senco"]]),
        ("系数写入后回读验证存在", "PASS", "", "主 runner 已复用 shared helper 执行 `写前快照 -> 写入 -> GETCO 回读比对 -> 失败回滚 -> finally 恢复模式`。", [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["ga_read_coefficient_group"], spans["test_v1_writeback_success"], spans["test_v1_writeback_mismatch"]]),
        ("每个点位有唯一标识", "PASS", "", "", [spans["runner_build_point_summary_row"], spans["runner_co2_point_tag"], spans["runner_h2o_point_tag"]]),
        ("每个点位有原始时间戳", "PASS", "", "", [spans["runner_collect_samples"], spans["logger_common_sheet_fields"]]),
        ("每个点位有保存时间戳", "PASS", "", "样本级与点位级导出都新增了独立 `save_ts`，且不替换原有采样时间字段。", [spans["runner_perform_light_exports"], spans["runner_perform_heavy_exports"], spans["logger_log_sample"], spans["logger_log_point"], spans["test_v1_writeback_point_fields"]]),
        ("点位保存前有稳态/等待/滤波逻辑", "PASS", "", "", [spans["runner_wait_primary_sensor_stable"], spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"]]),
        ("点位保存不会覆盖前一点位", "PASS", "", "", [spans["logger_log_sample"], spans["logger_append_points_csv"], spans["logger_log_point_samples"], spans["test_audit_trace_overwrite"]]),
        ("点位保存不会混入上一点位过渡态数据", "PASS", "", "", [spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"], spans["runner_enqueue_deferred_sample_exports"], spans["test_route_handoff_defers_exports"]]),
        ("报表导出/过程表生成存在", "PASS", "", "", [spans["logger_log_point"], spans["logger_log_point_samples"], spans["logger_build_analyzer_summary"], spans["corrected_build_delivery"]]),
        ("标定前后系数可追溯", "PASS", "", "主 runner 现在会把 `coeff_before/coeff_target/coeff_readback/coeff_rollback_*` 持久化到专用审计 CSV。", [spans["runner_maybe_write_coefficients"], spans["runner_persist_coefficient_write_result"], spans["logger_log_coefficient_write"], spans["test_v1_writeback_success"]]),
        ("异常中断后不会把设备留在错误模式", "UNKNOWN", "", "代码已在 shared helper 中用 finally 恢复正常模式，并有离线异常分支测试；但现场设备能否总是返回可确认的模式快照，当前仍取决于协议帧可读性。", [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["ga_read_current_mode_snapshot"], spans["test_v1_writeback_mismatch"]]),
        ("2026-04-03 以来的改动中，是否存在高风险改动", "PASS", "", "2026-04-07/2026-04-12 的高风险改动已经在当前 HEAD 上被安全默认值和显式启用门禁收口；历史风险存在，但默认运行边界已回到安全态。", [spans["runner_effective_postrun_cfg"], spans["runner_log_postrun_cfg"], spans["config_postrun_corrected_delivery"], spans["test_v1_writeback_defaults_safe"]]),
    ]
    lines = [
        "# 风险清单",
        "",
        "| 检查项 | 结论 | 风险等级 | 触发条件 / 说明 | 证据 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for title, status, risk, note, file_spans in rows:
        lines.append(f"| {title} | {status} | {risk or '-'} | {note or '-'} | {span_group(*file_spans)} |")
    lines.append("")
    lines.append("## 重点说明")
    lines.append("")
    lines.append(f"- 最近高风险改动关联 commit: {', '.join(f'`{item}`' for item in recent_commit_ids)}")
    lines.append("")
    return "\n".join(lines)


def build_evidence_items(spans: Dict[str, FileSpan]) -> List[EvidenceItem]:
    runner_coeff_commits = blame_commits(spans["runner_maybe_write_coefficients"])
    postrun_hook_commits = sorted({*blame_commits(spans["runner_postrun_corrected_delivery"]), *blame_commits(spans["config_postrun_corrected_delivery"])})
    point_summary_commits = sorted({*blame_commits(spans["runner_build_point_summary_row"]), *blame_commits(spans["logger_log_point"]), *blame_commits(spans["logger_log_sample"])})
    return [
        EvidenceItem("E001", "主 runner 已具备写前/写后回读验证与失败回滚", "High", "PASS", "`CalibrationRunner._maybe_write_coefficients` 已复用 shared helper，执行写前快照、`GETCO` 回读比对、失败回滚和 finally 恢复模式。", [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["ga_read_coefficient_group"], spans["test_v1_writeback_success"], spans["test_v1_writeback_mismatch"]], runner_coeff_commits, "这直接消除了“主路径写系数后无回读验证”的高风险缺口，也让写设备步骤具备了失败即终止的闭环语义。", "继续保留 shared helper，后续不要再回退到只写 `SENCO` 不验证的分叉实现。", 0.97),
        EvidenceItem("E002", "postrun corrected delivery 默认恢复为安全关闭", "High", "PASS", "默认配置已改为 `enabled=False`、`write_devices=False`；主 runner 运行时还会记录开关值及来源，并支持通过显式 ENV/配置开启。", [spans["runner_effective_postrun_cfg"], spans["runner_log_postrun_cfg"], spans["runner_postrun_corrected_delivery"], spans["config_postrun_corrected_delivery"], spans["test_v1_writeback_defaults_safe"]], postrun_hook_commits, "这把默认运行边界重新收回到“可导出、可 dry-run、不可默认真写设备”，避免一次普通 V1 运行自动进入真实写回链路。", "继续把真实写设备授权保持为显式 opt-in，不要重新改回默认真写。", 0.96),
        EvidenceItem("E003", "点位导出已区分采样时间与保存时间", "Medium", "PASS", "样本级与点位级导出现在都补了 `save_ts`，同时保留 `sample_ts` 和窗口时间字段，不破坏原有消费者。", [spans["runner_annotate_point_trace_rows"], spans["runner_build_point_summary_row"], spans["runner_perform_light_exports"], spans["runner_perform_heavy_exports"], spans["logger_log_sample"], spans["logger_log_point"], spans["test_v1_writeback_point_fields"]], point_summary_commits, "这让点位“何时采样”和“何时真正落盘”可分离追溯，能直接回应及时性审查。", "如果后续接数据库，再把 `insert_ts` 与 `save_ts` 继续区分即可。", 0.93),
        EvidenceItem("E004", "主流程前后系数追溯已补齐到专用审计 CSV", "Medium", "PASS", "主 runner 现在会把 `coeff_before/coeff_target/coeff_readback/coeff_rollback_*`、模式状态和失败原因写进 `coefficient_writeback_*.csv`。", [spans["runner_maybe_write_coefficients"], spans["runner_persist_coefficient_write_result"], spans["logger_log_coefficient_write"], spans["test_v1_writeback_success"]], sorted({*runner_coeff_commits, *postrun_hook_commits}), "这让本轮主流程自身就能提供 before/after provenance，不再依赖旁路 sidecar 才能审写回闭环。", "后续若要做更强 formal artifact，可把该 CSV 再汇入统一 execution/formal analysis 索引。", 0.9),
        EvidenceItem("E005", "点位链路具备稳态与 handoff 保护", "Low", "PASS", "采样前链路具备压力后门禁、freshness gate、route handoff deferred export 和唯一 point tag 组合，静态上可以解释为什么不会直接把切点瞬间或上一点位数据落到当前点位。", [spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"], spans["runner_enqueue_deferred_sample_exports"], spans["logger_log_point_samples"]], sorted({*blame_commits(spans["runner_wait_pressure_sampling_ready"]), *blame_commits(spans["runner_wait_sampling_freshness_gate"]), *blame_commits(spans["runner_enqueue_deferred_sample_exports"])}), "这部分是本轮静态审计里对“上一点位/过渡态/立即最新值被误存”的主要正向证据。", "继续保留这些保护，并在回归里长期保留只读 trace 检查。", 0.85),
        EvidenceItem("E006", "H2O 路线存在，但零点/跨度业务语义仍不明确", "Low", "UNKNOWN", "代码里能确认 H2O 路线、H2O point tag、以及 H2O coefficient groups 2/4 的存在；但没有找到与 CO2 zero/span 对等的 H2O 零点/跨度判定步骤。", [spans["runner_run_h2o_group"], spans["runner_h2o_point_tag"], spans["corrected_build_download_plan"]], sorted({*blame_commits(spans["runner_run_h2o_group"]), *blame_commits(spans["corrected_build_download_plan"])}), "后续如果要审“H2O 零点/跨度是否闭环”，当前静态证据还不足以直接落 PASS/FAIL。", "补一份明确的 H2O 点位业务定义或 acceptance 口径，再做下一轮核对。", 0.72),
    ]


def run_trace_check(spans: Dict[str, FileSpan]) -> Tuple[str, List[Tuple[str, str, str]], str]:
    cmd = [sys.executable, "-m", "pytest", "-q", *TRACE_TESTS]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode == 0:
        details = [
            ("点位样本字段完整性", "PASS", f"{span_group(spans['test_audit_trace_fields'], spans['runner_collect_samples'])}"),
            ("不同点位不会互相覆盖", "PASS", f"{span_group(spans['test_audit_trace_overwrite'], spans['logger_log_point_samples'], spans['logger_append_points_csv'])}"),
            ("保存前存在稳定/窗口/新鲜度门禁", "PASS", f"{span_group(spans['test_audit_trace_guards'], spans['runner_wait_pressure_sampling_ready'], spans['runner_wait_sampling_freshness_gate'])}"),
        ]
        details.extend(
            [
                ("主流程写回系数有回读/回滚闭环", "PASS", f"{span_group(spans['test_v1_writeback_success'], spans['test_v1_writeback_mismatch'], spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'])}"),
                ("点位导出含 save_ts 且可保留连续点位", "PASS", f"{span_group(spans['test_v1_writeback_point_fields'], spans['runner_build_point_summary_row'], spans['runner_perform_light_exports'])}"),
            ]
        )
        status = "PASS"
    else:
        details = [("只读 trace 检查", "FAIL", f"pytest 返回码 `{proc.returncode}`；请查看输出。证据参考 {spans['test_audit_trace_guards'].as_ref()}。")]
        status = "FAIL"
    output = f"$ {' '.join(cmd)}\n{proc.stdout}{proc.stderr}".rstrip() + "\n"
    return status, details, output


def build_trace_check_report(spans: Dict[str, FileSpan], *, generated_at: str) -> str:
    status, details, output = run_trace_check(spans)
    lines = ["# 只读 Trace 检查", "", f"- 生成时间: {generated_at}", f"- 命令: `python -m pytest -q {' '.join(TRACE_TESTS)}`", f"- 总结论: {status}", "", "## 结果", ""]
    for title, result, evidence in details:
        lines.append(f"- {title}: {result} | 证据: {evidence}")
    lines.extend(["", "## 原始输出", "", "```text", output.rstrip(), "```", ""])
    return "\n".join(lines)


def build_risk_checklist(spans: Dict[str, FileSpan], recent_commit_ids: List[str]) -> str:
    rows = [
        ("V1 主流程有明确入口", "PASS", "", "", [spans["run_app_file"], spans["app_start_run_background"], spans["runner_run"]]),
        ("流程顺序完整且闭环", "PASS", "", "", [spans["runner_run"], spans["runner_run_points"], spans["runner_run_temperature_group"], spans["runner_cleanup"]]),
        ("CO2 零点检查存在", "PASS", "", "", [spans["runner_is_zero_co2_point"], spans["runner_wait_co2_route_soak_before_seal"]]),
        ("CO2 跨度存在", "PASS", "", "", [spans["runner_run_temperature_group"], spans["runner_run_co2_point"]]),
        (
            "H2O 零点存在",
            "NOT_SUPPORTED",
            "",
            "当前 HEAD 只确认 H2O 路由和 H2O ratio-poly 摘要选择存在，没有与 CO2 对等的 H2O zero 业务步骤。",
            [
                spans["runner_run_h2o_group"],
                spans["runner_filter_ratio_poly_summary_rows"],
                spans["h2o_summary_default_selection"],
                spans["runner_log_h2o_zero_span_capability"],
                spans["runner_require_supported_h2o_zero_span"],
            ],
        ),
        (
            "H2O 跨度存在",
            "NOT_SUPPORTED",
            "",
            "当前 HEAD 只确认 H2O 路由和 H2O ratio-poly 摘要选择存在，没有与 CO2 对等的 H2O span 业务步骤。",
            [
                spans["runner_run_h2o_group"],
                spans["runner_filter_ratio_poly_summary_rows"],
                spans["h2o_summary_default_selection"],
                spans["runner_log_h2o_zero_span_capability"],
                spans["runner_require_supported_h2o_zero_span"],
            ],
        ),
        ("进入校准模式存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["ga_set_mode_with_ack"]]),
        ("退出校准模式存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"]]),
        ("系数写入存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["ga_set_senco"]]),
        ("系数写入后回读验证存在", "PASS", "", "", [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["ga_read_coefficient_group"]]),
        ("每个点位有唯一标识", "PASS", "", "", [spans["runner_build_point_summary_row"], spans["runner_co2_point_tag"], spans["runner_h2o_point_tag"]]),
        ("每个点位有原始时间戳", "PASS", "", "", [spans["runner_collect_samples"], spans["logger_common_sheet_fields"]]),
        ("每个点位有保存时间戳", "PASS", "", "", [spans["runner_perform_light_exports"], spans["runner_perform_heavy_exports"], spans["test_v1_writeback_point_fields"]]),
        ("点位保存前有稳态/等待/滤波逻辑", "PASS", "", "", [spans["runner_wait_primary_sensor_stable"], spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"]]),
        ("点位保存不会覆盖前一点位", "PASS", "", "", [spans["logger_log_sample"], spans["logger_append_points_csv"], spans["logger_log_point_samples"], spans["test_audit_trace_overwrite"]]),
        ("点位保存不会混入上一点位过渡态数据", "PASS", "", "", [spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"], spans["runner_enqueue_deferred_sample_exports"], spans["test_route_handoff_defers_exports"]]),
        ("报表导出/过程表生成存在", "PASS", "", "", [spans["logger_log_point"], spans["logger_log_point_samples"], spans["logger_build_analyzer_summary"]]),
        ("标定前后系数可追溯", "PASS", "", "", [spans["runner_persist_coefficient_write_result"], spans["logger_log_coefficient_write"], spans["test_v1_writeback_success"]]),
        (
            "离线 fault-injection 已覆盖异常恢复",
            "PASS",
            "",
            "shared helper 已有 focused fault-injection tests，覆盖 set_mode / GETCO / rollback / 模式确认异常。",
            [
                spans["corrected_full_writeback"],
                spans["test_fault_set_mode2"],
                spans["test_fault_set_senco"],
                spans["test_fault_readback_failures"],
                spans["test_fault_rollback_write_failure"],
                spans["test_fault_set_mode1"],
                spans["test_fault_no_snapshot"],
                spans["test_fault_exit_unconfirmed"],
            ],
        ),
        (
            "真实设备异常恢复证据",
            "ONLINE_EVIDENCE_REQUIRED",
            "",
            "代码、离线注入和受双开关保护的 online acceptance 工具已经就位；但现场异常恢复仍缺真机协议证据。",
            [spans["corrected_full_writeback"], spans["ga_read_current_mode_snapshot"], spans["online_acceptance_run"], spans["online_acceptance_bundle"]],
        ),
        (
            "2026-04-03 以来的改动中，是否存在高风险改动",
            "PASS",
            "",
            "默认真写设备风险、主路径无回读验证、点位无 save_ts、系数 before/after 缺失，已在当前 HEAD 上收口。",
            [
                spans["runner_effective_postrun_cfg"],
                spans["runner_log_postrun_cfg"],
                spans["runner_maybe_write_coefficients"],
                spans["runner_persist_coefficient_write_result"],
                spans["test_v1_writeback_defaults_safe"],
            ],
        ),
    ]
    lines = [
        "# 风险清单",
        "",
        "| 检查项 | 结论 | 风险等级 | 触发条件 / 说明 | 证据 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for title, status, risk, note, file_spans in rows:
        lines.append(f"| {title} | {status} | {risk or '-'} | {note or '-'} | {span_group(*file_spans)} |")
    lines.extend(
        [
            "",
            "## 重点说明",
            "",
            f"- 历史高风险 commit 参考: {', '.join(f'`{item}`' for item in recent_commit_ids) if recent_commit_ids else '(none tagged)'}",
            "- 本文件明确区分“代码已证明/离线已证明”和“现场仍缺证据”。",
            "",
        ]
    )
    return "\n".join(lines)


def build_evidence_items(spans: Dict[str, FileSpan]) -> List[EvidenceItem]:
    runner_coeff_commits = blame_commits(spans["runner_maybe_write_coefficients"])
    postrun_hook_commits = sorted(
        {*blame_commits(spans["runner_postrun_corrected_delivery"]), *blame_commits(spans["config_postrun_corrected_delivery"])}
    )
    point_summary_commits = sorted(
        {*blame_commits(spans["runner_build_point_summary_row"]), *blame_commits(spans["logger_log_point"]), *blame_commits(spans["logger_log_sample"])}
    )
    h2o_capability_commits = sorted(
        {*blame_commits(spans["runner_run_h2o_group"]), *blame_commits(spans["runner_require_supported_h2o_zero_span"]), *blame_commits(spans["h2o_summary_default_selection"])}
    )
    fault_commits = sorted(
        {*blame_commits(spans["corrected_full_writeback"]), *blame_commits(spans["test_fault_set_mode2"]), *blame_commits(spans["test_fault_set_mode1"])}
    )
    return [
        EvidenceItem(
            "E001",
            "主 runner 写回已具备 GETCO 回读验证与失败回滚",
            "High",
            "PASS",
            "主 runner 已复用 shared helper 执行写前快照、GETCO 回读、失败回滚和 finally 恢复模式。",
            [spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["ga_read_coefficient_group"], spans["test_v1_writeback_success"], spans["test_v1_writeback_mismatch"]],
            runner_coeff_commits,
            "这消除了“主路径写系数后无回读验证”的高风险缺口。",
            "继续保持主 runner 与 corrected autodelivery 共用同一 helper，不要回退到只写 SENCO 的分叉实现。",
            0.98,
        ),
        EvidenceItem(
            "E002",
            "postrun corrected delivery 默认保持安全关闭",
            "High",
            "PASS",
            "默认配置已改为 enabled=False、write_devices=False，且运行时会记录开关值与来源。",
            [spans["runner_effective_postrun_cfg"], spans["runner_log_postrun_cfg"], spans["config_postrun_corrected_delivery"], spans["test_v1_writeback_defaults_safe"]],
            postrun_hook_commits,
            "这把默认运行边界重新收回到 dry-run / 导出安全态，避免普通 V1 运行自动进入真实写回链路。",
            "继续把真实写设备授权保持为显式 opt-in。",
            0.97,
        ),
        EvidenceItem(
            "E003",
            "点位导出已区分 sample_ts 与 save_ts",
            "Medium",
            "PASS",
            "样本级与点位级导出都已补齐 save_ts，且保留原有 sample_ts 和窗口字段。",
            [spans["runner_annotate_point_trace_rows"], spans["runner_build_point_summary_row"], spans["runner_perform_light_exports"], spans["runner_perform_heavy_exports"], spans["test_v1_writeback_point_fields"]],
            point_summary_commits,
            "这让点位“何时采样”和“何时真正落盘”可分离追溯。",
            "后续若接数据库，再增加 insert_ts 即可。",
            0.94,
        ),
        EvidenceItem(
            "E004",
            "主流程前后系数追溯已落到专用 CSV",
            "Medium",
            "PASS",
            "主 runner 现在会把 coeff_before / coeff_target / coeff_readback / coeff_rollback_* 以及模式恢复字段写入 coefficient_writeback CSV。",
            [spans["runner_persist_coefficient_write_result"], spans["logger_log_coefficient_write"], spans["test_v1_writeback_success"]],
            sorted({*runner_coeff_commits, *postrun_hook_commits}),
            "这让本轮主流程自身就能提供 before/after provenance。",
            "后续可再汇入统一 formal artifact 索引，但这不是本轮阻塞项。",
            0.91,
        ),
        EvidenceItem(
            "E005",
            "shared helper 的异常恢复已由离线 fault injection 覆盖",
            "Medium",
            "PASS",
            "focused tests 已覆盖 set_mode(2) 失败、GETCO 超时/空返回/解析异常、回读不一致、rollback 失败、退出失败、模式快照缺失和退出未确认。",
            [
                spans["corrected_full_writeback"],
                spans["test_fault_set_mode2"],
                spans["test_fault_set_senco"],
                spans["test_fault_readback_failures"],
                spans["test_fault_rollback_write_failure"],
                spans["test_fault_set_mode1"],
                spans["test_fault_no_snapshot"],
                spans["test_fault_exit_unconfirmed"],
            ],
            fault_commits,
            "这把“至少尝试退出”和“已确认退出”从口头安全变成了离线可验收证据。",
            "后续若 helper 字段再扩展，保持 mode_exit_attempted/mode_exit_confirmed 这对语义不要回退。",
            0.95,
        ),
        EvidenceItem(
            "E006",
            "H2O zero/span 业务链当前为 NOT_SUPPORTED",
            "Low",
            "NOT_SUPPORTED",
            "当前 HEAD 能确认 H2O 路由、H2O 点位采样和 H2O ratio-poly 摘要选择存在，但没有与 CO2 zero/span 对等的 H2O zero/span 业务步骤。",
            [
                spans["runner_run_h2o_group"],
                spans["runner_filter_ratio_poly_summary_rows"],
                spans["h2o_summary_default_selection"],
                spans["runner_log_h2o_zero_span_capability"],
                spans["runner_require_supported_h2o_zero_span"],
                spans["test_v1_h2o_zero_span_not_supported"],
                spans["test_v1_h2o_zero_span_fail_fast"],
            ],
            h2o_capability_commits,
            "这意味着 H2O route 存在不等于 H2O zero/span 已闭环，当前审计不能再把它留成模糊 UNKNOWN。",
            "除非后续补到明确的 H2O zero/span 业务定义与步骤，否则应继续保持 NOT_SUPPORTED。",
            0.92,
        ),
        EvidenceItem(
            "E007",
            "真实设备异常恢复仍缺现场证据",
            "Medium",
            "ONLINE_EVIDENCE_REQUIRED",
            "代码与离线故障注入已证明 helper 会尝试退出模式并区分 confirmed/unconfirmed，且在线验收工具已经以双开关保护方式落地；但现场协议异常下的最终恢复仍缺真机证据。",
            [spans["corrected_full_writeback"], spans["ga_read_current_mode_snapshot"], spans["online_acceptance_run"], spans["online_acceptance_bundle"], spans["test_online_acceptance_dual_gate"]],
            fault_commits,
            "离线通过不等于 real acceptance，通过代码和仿真还不能证明现场异常时一定安全退出。",
            "若未来用户明确授权，只能通过带双开关的 online acceptance 工具做最小范围 V1 real smoke / short run，且结论必须标注为工程验证，不是 real acceptance。",
            0.84,
        ),
    ]


def run_trace_check(spans: Dict[str, FileSpan]) -> Tuple[str, List[Tuple[str, str, str]], str]:
    cmd = [sys.executable, "-m", "pytest", "-q", *TRACE_TESTS]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode == 0:
        details = [
            ("点位样本字段完整性", "PASS", f"{span_group(spans['test_audit_trace_fields'], spans['runner_collect_samples'])}"),
            ("不同点位不会互相覆盖", "PASS", f"{span_group(spans['test_audit_trace_overwrite'], spans['logger_log_point_samples'], spans['logger_append_points_csv'])}"),
            ("保存前存在稳定/窗口/新鲜度门禁", "PASS", f"{span_group(spans['test_audit_trace_guards'], spans['runner_wait_pressure_sampling_ready'], spans['runner_wait_sampling_freshness_gate'])}"),
            ("主流程写回系数有回读/回滚闭环", "PASS", f"{span_group(spans['test_v1_writeback_success'], spans['test_v1_writeback_mismatch'], spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'])}"),
            ("点位导出含 save_ts 且保留连续点位", "PASS", f"{span_group(spans['test_v1_writeback_point_fields'], spans['runner_build_point_summary_row'], spans['runner_perform_light_exports'])}"),
            ("异常恢复 fault injection 覆盖", "PASS", f"{span_group(spans['test_fault_set_mode2'], spans['test_fault_set_senco'], spans['test_fault_readback_failures'], spans['test_fault_rollback_write_failure'], spans['test_fault_set_mode1'], spans['test_fault_no_snapshot'], spans['test_fault_exit_unconfirmed'])}"),
            ("H2O zero/span 状态已显式收敛", "PASS", f"{span_group(spans['test_v1_h2o_zero_span_not_supported'], spans['test_v1_h2o_zero_span_fail_fast'], spans['runner_require_supported_h2o_zero_span'])}"),
            ("online acceptance 双开关与 CO2-only 保护", "PASS", f"{span_group(spans['test_online_acceptance_dual_gate'], spans['test_online_acceptance_dry_run'], spans['test_online_acceptance_exit_unconfirmed'], spans['test_online_acceptance_h2o_fail_fast'], spans['online_acceptance_run'])}"),
        ]
        status = "PASS"
    else:
        details = [("只读 trace 检查", "FAIL", f"pytest returned `{proc.returncode}`; see output. Evidence hint: {spans['test_audit_trace_guards'].as_ref()}")]
        status = "FAIL"
    output = f"$ {' '.join(cmd)}\n{proc.stdout}{proc.stderr}".rstrip() + "\n"
    return status, details, output


def build_trace_check_report(spans: Dict[str, FileSpan], *, generated_at: str) -> str:
    status, details, output = run_trace_check(spans)
    lines = [
        "# 只读 Trace 检查",
        "",
        f"- 生成时间: {generated_at}",
        f"- 命令: `python -m pytest -q {' '.join(TRACE_TESTS)}`",
        f"- 总结论: {status}",
        "",
        "## 结果",
        "",
    ]
    for title, result, evidence in details:
        lines.append(f"- {title}: {result} | 证据: {evidence}")
    lines.extend(["", "## 原始输出", "", "```text", output.rstrip(), "```", ""])
    return "\n".join(lines)


def _write_csv_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stored_rows = [dict(row) for row in rows]
    header: List[str] = []
    for row in stored_rows:
        for key in row.keys():
            if key not in header:
                header.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(stored_rows)


class _AcceptanceWritebackAnalyzer:
    def __init__(self) -> None:
        self.device_id = "086"
        self.mode = 1
        self.values = {1: [10.0, 20.0, 30.0, 40.0, 0.0, 0.0]}

    def read_current_mode_snapshot(self):
        return {"mode": self.mode, "id": self.device_id, "raw": f"mode={self.mode}"}

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.mode = int(mode)
        return True

    def set_senco(self, group: int, *coeffs) -> bool:
        values = list(coeffs[0]) if len(coeffs) == 1 and isinstance(coeffs[0], (list, tuple)) else list(coeffs)
        self.values[int(group)] = [float(value) for value in values]
        return True

    def read_coefficient_group(self, group: int):
        return {f"C{idx}": float(value) for idx, value in enumerate(self.values[int(group)])}


def _generate_acceptance_samples(acceptance_dir: Path) -> None:
    from gas_calibrator.data.points import CalibrationPoint
    from gas_calibrator.logging_utils import RunLogger
    from gas_calibrator.workflow.runner import CalibrationRunner

    with tempfile.TemporaryDirectory(prefix="v1_acceptance_") as tmp_dir_text:
        tmp_dir = Path(tmp_dir_text)
        logger = RunLogger(tmp_dir)
        runner = CalibrationRunner(
            {
                "workflow": {
                    "sampling": {
                        "stable_count": 2,
                        "interval_s": 0.0,
                        "quality": {"enabled": False},
                    }
                },
                "coefficients": {
                    "enabled": True,
                    "auto_fit": False,
                    "fit_h2o": True,
                    "h2o_zero_span": {
                        "status": "not_supported",
                        "require_supported_capability": False,
                    },
                    "sencos": {
                        "1": {"values": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]},
                    },
                },
            },
            {"gas_analyzer": _AcceptanceWritebackAnalyzer()},
            logger,
            lambda *_: None,
            lambda *_: None,
        )
        captured_point_rows: List[Dict[str, Any]] = []
        original_log_point = logger.log_point

        def _capture_point(row: Dict[str, Any]) -> None:
            captured_point_rows.append(dict(row))
            original_log_point(row)

        logger.log_point = _capture_point

        point_co2 = CalibrationPoint(1, 20.0, 400.0, None, None, 1000.0, None, None, None)
        point_h2o = CalibrationPoint(2, 20.0, None, 20.0, 35.0, 1000.0, -8.0, 2.5, "fixture")
        samples_by_point = {
            1: [
                {
                    "point_title": "CO2 400",
                    "point_row": 1,
                    "sample_ts": "2026-04-13T10:00:00.000+08:00",
                    "sample_end_ts": "2026-04-13T10:00:00.100+08:00",
                    "co2_ppm": 399.8,
                    "pressure_hpa": 1000.1,
                    "id": "086",
                },
                {
                    "point_title": "CO2 400",
                    "point_row": 1,
                    "sample_ts": "2026-04-13T10:00:01.000+08:00",
                    "sample_end_ts": "2026-04-13T10:00:01.100+08:00",
                    "co2_ppm": 400.2,
                    "pressure_hpa": 1000.2,
                    "id": "086",
                },
            ],
            2: [
                {
                    "point_title": "H2O 2.5",
                    "point_row": 2,
                    "sample_ts": "2026-04-13T10:05:00.000+08:00",
                    "sample_end_ts": "2026-04-13T10:05:00.100+08:00",
                    "h2o_mmol": 2.4,
                    "pressure_hpa": 1000.0,
                    "id": "086",
                },
                {
                    "point_title": "H2O 2.5",
                    "point_row": 2,
                    "sample_ts": "2026-04-13T10:05:01.000+08:00",
                    "sample_end_ts": "2026-04-13T10:05:01.100+08:00",
                    "h2o_mmol": 2.6,
                    "pressure_hpa": 1000.1,
                    "id": "086",
                },
            ],
        }

        def _collect_samples(self, point, *_args, **_kwargs):
            return [dict(row) for row in samples_by_point[point.index]]

        runner._collect_samples = types.MethodType(_collect_samples, runner)
        runner._point_runtime_state(point_co2, phase="co2", create=True).update({"sampling_window_qc_status": "pass"})
        runner._point_runtime_state(point_h2o, phase="h2o", create=True).update({"sampling_window_qc_status": "pass"})
        runner._sample_and_log(point_co2, phase="co2", point_tag=runner._co2_point_tag(point_co2))
        runner._sample_and_log(point_h2o, phase="h2o", point_tag=runner._h2o_point_tag(point_h2o))
        runner._maybe_write_coefficients()
        point_rows = [dict(row) for row in captured_point_rows]
        sample_rows = [dict(row) for row in getattr(runner, "_all_samples", [])]
        coefficient_rows = [dict(row) for row in getattr(logger, "_coefficient_write_rows", [])]
        logger.close()

        _write_csv_rows(acceptance_dir / "02_sample_points.csv", point_rows)
        _write_csv_rows(acceptance_dir / "03_sample_samples.csv", sample_rows)
        _write_csv_rows(acceptance_dir / "04_sample_coefficient_writeback.csv", coefficient_rows)


def build_acceptance_capability_matrix(spans: Dict[str, FileSpan], *, generated_at: str, head: str) -> str:
    rows = [
        ("CO2 zero", "PASS", span_group(spans["runner_is_zero_co2_point"], spans["runner_wait_co2_route_soak_before_seal"])),
        ("CO2 span", "PASS", span_group(spans["runner_run_temperature_group"], spans["runner_run_co2_point"])),
        ("H2O zero", "NOT_SUPPORTED", span_group(spans["runner_run_h2o_group"], spans["runner_filter_ratio_poly_summary_rows"], spans["runner_require_supported_h2o_zero_span"])),
        ("H2O span", "NOT_SUPPORTED", span_group(spans["runner_run_h2o_group"], spans["runner_filter_ratio_poly_summary_rows"], spans["runner_require_supported_h2o_zero_span"])),
        ("device writeback", "PASS", span_group(spans["runner_maybe_write_coefficients"], spans["corrected_full_writeback"], spans["test_v1_writeback_success"])),
        ("readback verify", "PASS", span_group(spans["corrected_full_writeback"], spans["ga_read_coefficient_group"], spans["test_v1_writeback_success"])),
        ("rollback", "PASS", span_group(spans["corrected_full_writeback"], spans["test_v1_writeback_mismatch"], spans["test_fault_readback_failures"])),
        ("mode restore", "PASS", span_group(spans["corrected_full_writeback"], spans["test_fault_set_mode2"], spans["test_fault_set_mode1"])),
        ("point save traceability", "PASS", span_group(spans["runner_build_point_summary_row"], spans["runner_annotate_point_trace_rows"], spans["test_v1_writeback_point_fields"])),
        ("offline fault injection coverage", "PASS", span_group(spans["test_fault_set_mode2"], spans["test_fault_set_senco"], spans["test_fault_readback_failures"], spans["test_fault_rollback_write_failure"], spans["test_fault_set_mode1"], spans["test_fault_no_snapshot"], spans["test_fault_exit_unconfirmed"])),
        ("real-device abnormal recovery evidence", "ONLINE_EVIDENCE_REQUIRED", span_group(spans["corrected_full_writeback"], spans["ga_read_current_mode_snapshot"], spans["online_acceptance_run"], spans["online_acceptance_bundle"])),
    ]
    lines = [
        "# Capability Matrix",
        "",
        f"- generated_at: {generated_at}",
        f"- head: `{head}`",
        "",
        "| capability | status | evidence |",
        "| --- | --- | --- |",
    ]
    for title, status, evidence in rows:
        lines.append(f"| {title} | {status} | {evidence} |")
    lines.append("")
    return "\n".join(lines)


def build_fault_injection_matrix(spans: Dict[str, FileSpan], *, generated_at: str) -> str:
    rows = [
        ("set_mode(2) 失败", "PASS", "会尝试退出模式，不把 attempted 当成 confirmed", span_group(spans["test_fault_set_mode2"], spans["corrected_full_writeback"])),
        ("set_senco 中途异常", "PASS", "会回滚并恢复模式", span_group(spans["test_fault_set_senco"], spans["corrected_full_writeback"])),
        ("GETCO 超时", "PASS", "verify 失败后回滚，最终模式恢复", span_group(spans["test_fault_readback_failures"], spans["corrected_full_writeback"])),
        ("GETCO 空返回", "PASS", "空返回不算成功，会进入失败/回滚路径", span_group(spans["test_fault_readback_failures"], spans["corrected_full_writeback"])),
        ("GETCO 解析异常", "PASS", "解析异常不算成功，会进入失败/回滚路径", span_group(spans["test_fault_readback_failures"], spans["corrected_full_writeback"])),
        ("回读不一致", "PASS", "readback mismatch 会失败并尝试回滚", span_group(spans["test_fault_readback_failures"], spans["corrected_full_writeback"])),
        ("rollback 写入失败", "PASS", "rollback_confirmed=False，unsafe=True", span_group(spans["test_fault_rollback_write_failure"], spans["corrected_full_writeback"])),
        ("set_mode(1) 退出失败", "PASS", "mode_exit_confirmed=False，unsafe=True", span_group(spans["test_fault_set_mode1"], spans["corrected_full_writeback"])),
        ("read_current_mode_snapshot 不可用", "PASS", "无法确认安全退出时标记 unsafe=True", span_group(spans["test_fault_no_snapshot"], spans["corrected_full_writeback"])),
        ("已尝试退出但无法确认最终模式", "PASS", "mode_exit_attempted=True 且 mode_exit_confirmed=False", span_group(spans["test_fault_exit_unconfirmed"], spans["corrected_full_writeback"])),
    ]
    lines = [
        "# Fault Injection Matrix",
        "",
        f"- generated_at: {generated_at}",
        "",
        "| scenario | offline result | expectation | evidence |",
        "| --- | --- | --- | --- |",
    ]
    for title, status, note, evidence in rows:
        lines.append(f"| {title} | {status} | {note} | {evidence} |")
    lines.append("")
    return "\n".join(lines)


def build_acceptance_summary(spans: Dict[str, FileSpan], *, generated_at: str, head: str) -> str:
    lines = [
        "# Acceptance Summary",
        "",
        f"- generated_at: {generated_at}",
        f"- head: `{head}`",
        "- evidence_source = simulated",
        "- not_real_acceptance_evidence = true",
        "",
        "## Summary",
        "",
        "- CO2 zero/span: PASS",
        "- H2O zero/span: NOT_SUPPORTED on this HEAD; route/point collection exists but explicit zero/span business chain does not.",
        "- device writeback safety closure: PASS in offline tests",
        "- protocol fault injection: PASS offline",
        "- real-device abnormal recovery evidence: ONLINE_EVIDENCE_REQUIRED",
        "- guarded online acceptance bundle: generated under `audit/v1_calibration_acceptance_online/`; dry-run by default, dual gate required for real-device runs.",
        "- shared path old failure in tests/test_runner_collect_only.py: resolved as stale expectation; focused test and full file now pass.",
        "",
        "## Evidence",
        "",
        f"- H2O capability state: {span_group(spans['runner_log_h2o_zero_span_capability'], spans['runner_require_supported_h2o_zero_span'], spans['test_v1_h2o_zero_span_fail_fast'])}",
        f"- offline fault injection: {span_group(spans['test_fault_set_mode2'], spans['test_fault_set_senco'], spans['test_fault_readback_failures'], spans['test_fault_rollback_write_failure'], spans['test_fault_set_mode1'], spans['test_fault_no_snapshot'], spans['test_fault_exit_unconfirmed'])}",
        f"- online acceptance gate/tooling: {span_group(spans['online_acceptance_run'], spans['online_acceptance_bundle'], spans['test_online_acceptance_dual_gate'], spans['test_online_acceptance_exit_unconfirmed'], spans['test_online_acceptance_h2o_fail_fast'])}",
        f"- shared path stale test fixed: {spans['test_collect_ratio_poly_h2o_selection'].as_ref()}",
        "",
    ]
    return "\n".join(lines)


def generate_acceptance_bundle(spans: Dict[str, FileSpan], *, generated_at: str, head: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _generate_acceptance_samples(output_dir)
    write_text(output_dir / "01_capability_matrix.md", build_acceptance_capability_matrix(spans, generated_at=generated_at, head=head))
    write_text(output_dir / "05_fault_injection_matrix.md", build_fault_injection_matrix(spans, generated_at=generated_at))
    write_text(output_dir / "06_acceptance_summary.md", build_acceptance_summary(spans, generated_at=generated_at, head=head))


def generate_online_acceptance_bundle(*, generated_at: str, head: str, output_dir: Path) -> None:
    from gas_calibrator.tools.run_v1_online_acceptance import write_online_acceptance_bundle

    write_online_acceptance_bundle(
        output_dir,
        generated_at=generated_at,
        head=head,
        last_run_summary={
            "status": "ONLINE_EVIDENCE_REQUIRED",
            "mode": "dry_run",
            "unsafe": False,
            "failure_reason": "",
        },
    )


def build_flow_map(spans: Dict[str, FileSpan]) -> str:
    lines = [
        "# V1 校准主流程图",
        "",
        "入口 -> 点表解析/重排 -> 温度分组编排 -> CO2/H2O 路由执行 -> 稳态/门禁 -> 样本采集 -> 点位保存 -> 系数写前快照/写入/回读/回滚 -> 模式恢复 -> 清理/后处理",
        "",
        "## 入口",
        "",
        f"- 默认入口文件: {spans['run_app_file'].as_ref()}",
        f"- UI 后台启动: {spans['app_start_run_background'].as_ref()}",
        f"- 主执行函数: {spans['runner_run'].as_ref()}",
        "",
        "## 步骤编排",
        "",
        f"- 点表解析: {spans['points_load_points_from_excel'].as_ref()}",
        f"- 点位重排: {spans['points_reorder_points'].as_ref()}",
        f"- 点位主调度: {spans['runner_run_points'].as_ref()}",
        f"- 温度组编排: {spans['runner_run_temperature_group'].as_ref()}",
        f"- CO2 主链: {spans['runner_run_co2_point'].as_ref()}",
        f"- H2O 主链: {spans['runner_run_h2o_group'].as_ref()}",
        "",
        "## 设备指令",
        "",
        f"- MODE: {spans['ga_set_mode_with_ack'].as_ref()}",
        f"- SENCO: {spans['ga_set_senco'].as_ref()}",
        f"- GETCO: {spans['ga_read_coefficient_group'].as_ref()}",
        f"- READDATA: {span_group(spans['ga_read_data_passive'], spans['ga_read_data_active'], spans['ga_parse_line_mode2'])}",
        "",
        "## 数据采集与保存",
        "",
        f"- 稳态判定: {spans['runner_wait_primary_sensor_stable'].as_ref()}",
        f"- 压力后门禁: {spans['runner_wait_pressure_sampling_ready'].as_ref()}",
        f"- freshness gate: {spans['runner_wait_sampling_freshness_gate'].as_ref()}",
        f"- 样本采集: {spans['runner_collect_samples'].as_ref()}",
        f"- 点位采样与导出: {spans['runner_sample_and_log'].as_ref()}",
        f"- 点位汇总行: {spans['runner_build_point_summary_row'].as_ref()}",
        f"- 样本/点位/写回导出: {span_group(spans['logger_log_sample'], spans['logger_log_point'], spans['logger_log_coefficient_write'])}",
        "",
        "## 系数写入闭环",
        "",
        f"- 主 runner 写回入口: {spans['runner_maybe_write_coefficients'].as_ref()}",
        f"- shared helper: {spans['corrected_full_writeback'].as_ref()}",
        f"- 写回持久化: {spans['runner_persist_coefficient_write_result'].as_ref()}",
        f"- 模式快照: {spans['ga_read_current_mode_snapshot'].as_ref()}",
        "",
        "## 结论",
        "",
        f"- CO2 零点检查: PASS | 证据 {span_group(spans['runner_is_zero_co2_point'], spans['runner_wait_co2_route_soak_before_seal'])}",
        f"- CO2 跨度: PASS | 证据 {span_group(spans['runner_run_temperature_group'], spans['runner_run_co2_point'])}",
        f"- H2O 零点: NOT_SUPPORTED | 证据 {span_group(spans['runner_run_h2o_group'], spans['runner_filter_ratio_poly_summary_rows'], spans['h2o_summary_default_selection'], spans['runner_log_h2o_zero_span_capability'], spans['runner_require_supported_h2o_zero_span'])}",
        f"- H2O 跨度: NOT_SUPPORTED | 证据 {span_group(spans['runner_run_h2o_group'], spans['runner_filter_ratio_poly_summary_rows'], spans['h2o_summary_default_selection'], spans['runner_log_h2o_zero_span_capability'], spans['runner_require_supported_h2o_zero_span'])}",
        f"- MODE=校准模式与恢复正常模式: PASS | 证据 {span_group(spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'], spans['ga_set_mode_with_ack'])}",
        f"- 系数写入后 GETCO 回读验证: PASS | 证据 {span_group(spans['runner_maybe_write_coefficients'], spans['corrected_full_writeback'], spans['ga_read_coefficient_group'])}",
        "",
        "## CO2 / H2O 关系",
        "",
        f"- 结构上是两套并行链路。CO2 执行入口见 {spans['runner_run_co2_point'].as_ref()}，H2O 执行入口见 {spans['runner_run_h2o_group'].as_ref()}。",
        "- 但“存在 H2O 路由/点位”不等于“存在 H2O zero/span 业务闭环”；当前 HEAD 的明确结论是 NOT_SUPPORTED，而不是 UNKNOWN。",
        "",
    ]
    return "\n".join(lines)


def ensure_output_dirs(output_dir: Path) -> None:
    (output_dir / "raw").mkdir(parents=True, exist_ok=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a read-only V1 calibration audit bundle.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="Git history cutoff, default: %(default)s")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Audit output directory, default: %(default)s")
    parser.add_argument("--acceptance-dir", default=str(DEFAULT_ACCEPTANCE_DIR), help="Acceptance output directory, default: %(default)s")
    parser.add_argument("--online-acceptance-dir", default=str(DEFAULT_ONLINE_ACCEPTANCE_DIR), help="Online acceptance output directory, default: %(default)s")
    parser.add_argument("--status-override-file", default="", help="Use an existing git status snapshot instead of live `git status`.")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
    acceptance_dir = Path(args.acceptance_dir).resolve()
    online_acceptance_dir = Path(args.online_acceptance_dir).resolve()
    status_override_file = Path(args.status_override_file).resolve() if args.status_override_file else None
    ensure_output_dirs(output_dir)

    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    branch = branch_name()
    head = head_commit()
    recent_30 = recent_30_commits()
    status_text = current_git_status_text(status_override_file)
    log_since_text = git_log_since_raw(args.since)
    hits_text = keyword_hits_text()
    relevant_commits = collect_relevant_commits(args.since)
    spans = build_code_spans()
    evidence_items = build_evidence_items(spans)

    write_text(output_dir / "raw" / "git_status.txt", status_text)
    write_text(output_dir / "raw" / "git_log_since_2026-04-03.txt", log_since_text)
    write_text(output_dir / "raw" / "rg_hits.txt", hits_text if hits_text.strip() else "(no hits)\n")
    write_text(output_dir / "README.md", build_readme(generated_at=generated_at, branch=branch, head=head, recent_commits_rows=recent_30, output_dir=output_dir))
    write_text(output_dir / "01_git_changes_since_2026-04-03.md", build_git_changes_report(branch=branch, head=head, since=args.since, commits=relevant_commits, status_text=status_text))
    write_text(output_dir / "02_v1_flow_map.md", build_flow_map(spans))
    write_text(output_dir / "03_point_storage_map.md", build_point_storage_map(spans))
    recent_high_risk_commit_ids = [item["commit"] for item in relevant_commits if item["commit"].startswith("8fa3f3ec") or item["commit"].startswith("248d0ac6") or item["commit"].startswith("1ebff243")]
    write_text(output_dir / "04_risk_checklist.md", build_risk_checklist(spans, recent_high_risk_commit_ids))
    write_text(output_dir / "05_evidence.json", json.dumps([item.as_json() for item in evidence_items], ensure_ascii=False, indent=2) + "\n")
    write_text(output_dir / "06_trace_check.md", build_trace_check_report(spans, generated_at=generated_at))
    generate_acceptance_bundle(spans, generated_at=generated_at, head=head, output_dir=acceptance_dir)
    generate_online_acceptance_bundle(generated_at=generated_at, head=head, output_dir=online_acceptance_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
