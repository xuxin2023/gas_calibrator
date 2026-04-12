"""Read-only V1 calibration audit report generator."""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "audit" / "v1_calibration_audit"
DEFAULT_SINCE = "2026-04-03 00:00:00"
TRACE_TEST = "tests/test_audit_v1_trace_check.py"

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
        "runner_postrun_corrected_delivery": symbol_span("src/gas_calibrator/workflow/runner.py", "CalibrationRunner._maybe_run_postrun_corrected_delivery"),
        "points_calibration_point": symbol_span("src/gas_calibrator/data/points.py", "CalibrationPoint"),
        "points_load_points_from_excel": symbol_span("src/gas_calibrator/data/points.py", "load_points_from_excel"),
        "points_reorder_points": symbol_span("src/gas_calibrator/data/points.py", "reorder_points"),
        "ga_set_mode_with_ack": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.set_mode_with_ack"),
        "ga_set_senco": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.set_senco"),
        "ga_read_coefficient_group": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_coefficient_group"),
        "ga_read_data_passive": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_data_passive"),
        "ga_read_data_active": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.read_data_active"),
        "ga_parse_line_mode2": symbol_span("src/gas_calibrator/devices/gas_analyzer.py", "GasAnalyzer.parse_line_mode2"),
        "logger_field_labels": bracket_block_span("src/gas_calibrator/logging_utils.py", "_FIELD_LABELS = {", "{", "}"),
        "logger_common_sheet_fields": bracket_block_span("src/gas_calibrator/logging_utils.py", "_COMMON_SHEET_FIELDS = [", "[", "]"),
        "logger_runlogger_init": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.__init__"),
        "logger_log_sample": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_sample"),
        "logger_log_point": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_point"),
        "logger_append_points_csv": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger._append_points_csv_row"),
        "logger_log_point_samples": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger.log_point_samples"),
        "logger_build_analyzer_summary": symbol_span("src/gas_calibrator/logging_utils.py", "RunLogger._build_analyzer_summary_row"),
        "corrected_build_download_plan": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "build_corrected_download_plan_rows"),
        "corrected_write_coefficients": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "write_coefficients_to_live_devices"),
        "corrected_read_group_retry": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "_read_group_with_match_retry"),
        "corrected_build_delivery": symbol_span("src/gas_calibrator/tools/run_v1_corrected_autodelivery.py", "build_corrected_delivery"),
        "config_postrun_corrected_delivery": bracket_block_span("src/gas_calibrator/config.py", '"postrun_corrected_delivery": {', "{", "}"),
        "test_collect_samples_h2o_snapshot": symbol_span("tests/test_runner_collect_only.py", "test_collect_samples_uses_preseal_dewpoint_snapshot_for_h2o"),
        "test_route_handoff_defers_exports": symbol_span("tests/test_runner_route_handoff.py", "test_sample_and_log_arms_route_handoff_before_sample_exports"),
        "test_audit_trace_fields": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_point_trace_row_contains_expected_fields"),
        "test_audit_trace_overwrite": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_point_trace_distinct_points_do_not_overwrite_each_other"),
        "test_audit_trace_guards": symbol_span("tests/test_audit_v1_trace_check.py", "test_v1_trace_code_keeps_stability_and_freshness_guards"),
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
        f"- `python -m pytest -q {TRACE_TEST}`",
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
        f"- 主 runner 的系数写入路径: {spans['runner_maybe_write_coefficients'].as_ref()}；会 `MODE=2 -> SENCO -> MODE=1`，但该路径没有调用 `GETCO` 回读比较。",
        f"- corrected autodelivery 旁路: {span_group(spans['corrected_build_download_plan'], spans['corrected_write_coefficients'], spans['corrected_read_group_retry'])}；这里对 CO2 使用 1/3 组、对 H2O 使用 2/4 组，并执行 `GETCO` 回读匹配。",
        f"- 运行结束后自动触发 corrected autodelivery 的 hook: {spans['runner_postrun_corrected_delivery'].as_ref()}。",
        f"- 清理与基线恢复: {span_group(spans['runner_cleanup'], spans['runner_restore_baseline'])}。",
        "",
        "## 明确结论",
        "",
        f"- CO2 零点检查: 有。见 {span_group(spans['runner_is_zero_co2_point'], spans['runner_wait_co2_route_soak_before_seal'])}。",
        f"- CO2 标气跨度: 有。`_run_temperature_group` 会按 CO2 源点 ppm 扫描，`_run_co2_point` 负责执行。见 {span_group(spans['runner_run_temperature_group'], spans['runner_run_co2_point'])}。",
        f"- H2O 零点/跨度: 只能确认 H2O 路线与多组湿度点存在，未找到明确以“零点/跨度”命名或判定的业务步骤；见 {spans['runner_run_h2o_group'].as_ref()}。结论: UNKNOWN。",
        f"- MODE=校准模式 与恢复正常模式: 有。主 runner 在写 SENCO 时 `MODE=2 -> MODE=1`，见 {spans['runner_maybe_write_coefficients'].as_ref()}。",
        f"- 系数写入后 GETCO 或等价回读验证: 旁路 corrected autodelivery 有，主 runner 当前主路径没有。主路径结论: FAIL；旁路能力见 {spans['corrected_write_coefficients'].as_ref()}。",
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
        f"- 保存到哪里: `samples_*.csv`、`point_XXXX*_samples.csv`、`points_*.csv`、`points_readable_*.csv/.xlsx`、`分析仪汇总_*.csv/.xlsx`，见 {span_group(spans['logger_runlogger_init'], spans['logger_log_sample'], spans['logger_log_point'], spans['logger_log_point_samples'], spans['logger_build_analyzer_summary'])}。",
        "",
        "## 保存 payload",
        "",
        f"- 样本级 payload 关键字段: `point_title/sample_ts/sample_start_ts/sample_end_ts/point_phase/point_tag/point_row/co2_ppm_target/h2o_mmol_target/pressure_target_hpa/co2_ppm/h2o_mmol/pressure_hpa/pressure_gauge_hpa/dewpoint_sample_ts/...`，见 {span_group(spans['runner_collect_samples'], spans['logger_common_sheet_fields'], spans['logger_field_labels'])}。",
        f"- 点位汇总 payload 关键字段: `point_row/point_phase/point_tag/targets/mean/std/valid_count/quality/timing`，见 {spans['runner_build_point_summary_row'].as_ref()}。",
        "",
        "## 必答问题",
        "",
        f"- 每个点位是否有唯一标识: PASS。当前实现依赖 `point_row + point_phase + point_tag` 组合，而不是单独 UUID；构造点与 tag 见 {span_group(spans['runner_build_co2_pressure_point'], spans['runner_build_h2o_pressure_point'], spans['runner_co2_point_tag'], spans['runner_h2o_point_tag'], spans['runner_build_point_summary_row'])}。",
        f"- 是否保存 raw timestamp 和 save timestamp: 部分 FAIL。样本有 `sample_ts`、设备采样时间戳和 `sample_end_ts`，见 {span_group(spans['runner_collect_samples'], spans['logger_common_sheet_fields'])}；但点位/样本导出没有单独的 `save_ts`/`insert_ts` 字段，见 {span_group(spans['logger_log_sample'], spans['logger_log_point'], spans['runner_build_point_summary_row'])}。",
        f"- 是否区分采样时间与入库时间: FAIL。当前只持久化采样相关时间，没有单独入库时间戳，见 {span_group(spans['runner_collect_samples'], spans['runner_build_point_summary_row'])}。",
        f"- 是否可能把“最新一条高频数据”误存成当前点位: 当前代码有 freshness gate 与压力后门禁，结论 PASS，但仅限静态审计；证据见 {span_group(spans['runner_wait_sampling_freshness_gate'], spans['runner_wait_pressure_sampling_ready'], spans['test_audit_trace_guards'])}。",
        f"- 是否可能上一点位/过渡态/未稳定数据被保存到下一点位: 代码有 route handoff + deferred export 保护，结论 PASS；见 {span_group(spans['runner_enqueue_deferred_sample_exports'], spans['runner_flush_deferred_sample_exports'], spans['runner_enqueue_deferred_point_exports'], spans['runner_flush_deferred_point_exports'], spans['test_route_handoff_defers_exports'])}。",
        f"- 是否可能覆盖前一个点位，而不是新增一条: 对“不同点位”结论 PASS。`samples.csv/points.csv` 追加写入，单点样本文件按 `point_row + phase + tag` 分文件，离线测试见 {span_group(spans['logger_log_sample'], spans['logger_append_points_csv'], spans['logger_log_point_samples'], spans['test_audit_trace_overwrite'])}。但如果同一 `point_row + phase + tag` 被重复导出，单点样本 CSV 会覆盖同名文件，这属于同标识重写，不是不同点位覆盖。",
        f"- 是否能追溯标定前系数、标定后系数、上一次系数: 主 runner 结论 FAIL。主路径只写 `SENCO`，没有在本流程里保存 before/after snapshot；见 {spans['runner_maybe_write_coefficients'].as_ref()}。旁路工具 `run_v1_merged_calibration_sidecar.py` 可以单独读 before/after，但不是主 runner 自动路径。",
        f"- CO2 和 H2O 两套点位表结构是否一致: 样本主结构大体一致，共用 `COMMON_SHEET_FIELDS`，但目标字段和 H2O 预封压露点快照有差异；见 {span_group(spans['logger_common_sheet_fields'], spans['runner_collect_samples'], spans['test_collect_samples_h2o_snapshot'])}。",
        "",
        "## 点位存储风险表",
        "",
        "| 项目 | 结论 | 证据 |",
        "| --- | --- | --- |",
        f"| 点位唯一标识 | PASS | {span_group(spans['runner_build_point_summary_row'], spans['runner_co2_point_tag'], spans['runner_h2o_point_tag'])} |",
        f"| 样本原始时间戳 | PASS | {span_group(spans['runner_collect_samples'], spans['logger_common_sheet_fields'])} |",
        f"| 保存时间戳 | FAIL | {span_group(spans['logger_log_sample'], spans['logger_log_point'], spans['runner_build_point_summary_row'])} |",
        f"| 采样/入库时间区分 | FAIL | {span_group(spans['runner_collect_samples'], spans['runner_build_point_summary_row'])} |",
        f"| 切点后立即取最新值 | PASS | {span_group(spans['runner_wait_pressure_sampling_ready'], spans['runner_wait_sampling_freshness_gate'], spans['test_audit_trace_guards'])} |",
        f"| 过渡态混入下一点位 | PASS | {span_group(spans['runner_enqueue_deferred_sample_exports'], spans['runner_flush_deferred_sample_exports'], spans['test_route_handoff_defers_exports'])} |",
        f"| 不同点位互相覆盖 | PASS | {span_group(spans['logger_log_point_samples'], spans['test_audit_trace_overwrite'])} |",
        f"| 系数 before/after 追溯 | FAIL | {span_group(spans['runner_maybe_write_coefficients'], spans['corrected_build_delivery'])} |",
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
        ("系数写入后回读验证存在", "FAIL", "High", "主 runner 的 `_maybe_write_coefficients` 只执行 `MODE=2 -> SENCO -> MODE=1`，没有 `GETCO`/等价比对；如果写入被设备部分接受、截断或被旧值覆盖，本流程自己无法发现。", [spans["runner_maybe_write_coefficients"], spans["ga_read_coefficient_group"], spans["corrected_write_coefficients"]]),
        ("每个点位有唯一标识", "PASS", "", "", [spans["runner_build_point_summary_row"], spans["runner_co2_point_tag"], spans["runner_h2o_point_tag"]]),
        ("每个点位有原始时间戳", "PASS", "", "", [spans["runner_collect_samples"], spans["logger_common_sheet_fields"]]),
        ("每个点位有保存时间戳", "FAIL", "Medium", "CSV/XLSX 只保存采样时间与设备时间，没有单独 `save_ts`/`insert_ts`；审查“何时落盘”时无法和采样时间区分。", [spans["runner_collect_samples"], spans["runner_build_point_summary_row"], spans["logger_log_sample"], spans["logger_log_point"]]),
        ("点位保存前有稳态/等待/滤波逻辑", "PASS", "", "", [spans["runner_wait_primary_sensor_stable"], spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"]]),
        ("点位保存不会覆盖前一点位", "PASS", "", "", [spans["logger_log_sample"], spans["logger_append_points_csv"], spans["logger_log_point_samples"], spans["test_audit_trace_overwrite"]]),
        ("点位保存不会混入上一点位过渡态数据", "PASS", "", "", [spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"], spans["runner_enqueue_deferred_sample_exports"], spans["test_route_handoff_defers_exports"]]),
        ("报表导出/过程表生成存在", "PASS", "", "", [spans["logger_log_point"], spans["logger_log_point_samples"], spans["logger_build_analyzer_summary"], spans["corrected_build_delivery"]]),
        ("标定前后系数可追溯", "FAIL", "Medium", "主 runner 不自动保存 before/after coefficient snapshot；如果只保留本轮主流程产物，无法直接追到写前系数。独立 sidecar 可以做 before/after，但未接入主 runner。", [spans["runner_maybe_write_coefficients"], spans["runner_postrun_corrected_delivery"], spans["corrected_build_delivery"]]),
        ("异常中断后不会把设备留在错误模式", "UNKNOWN", "", "", [spans["runner_cleanup"], spans["runner_restore_baseline"], spans["runner_maybe_write_coefficients"]]),
        ("2026-04-03 以来的改动中，是否存在高风险改动", "FAIL", "High", "2026-04-07 起把 postrun corrected delivery 接进主 runner，2026-04-12 又把默认配置改成 `enabled=True`、`write_devices=True`、`verify_short_run.enabled=True`。这会让一次完成的 V1 运行自动进入后处理写回/短验证链路，风险边界明显扩大。", [spans["runner_postrun_corrected_delivery"], spans["config_postrun_corrected_delivery"]]),
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
        EvidenceItem("E001", "主 runner 写系数后缺少回读验证", "High", "FAIL", "`CalibrationRunner._maybe_write_coefficients` 只做 MODE/SENCO 写入与模式退出，没有调用 GETCO 或等价比较；而驱动和 corrected autodelivery 旁路已经具备 GETCO readback 能力。", [spans["runner_maybe_write_coefficients"], spans["ga_read_coefficient_group"], spans["corrected_write_coefficients"]], runner_coeff_commits, "如果 CO2/H2O 系数写入被部分接受、设备回写了旧值、或写入序列被串口噪声打断，本轮 V1 主流程自身无法发现，闭环证据不成立。", "先在主 runner 的系数写入路径补齐 GETCO readback 与差异落盘；至少把写入前/写入后值、比较结果写到 formal artifact。", 0.97),
        EvidenceItem("E002", "postrun corrected delivery 已接入主 runner 且默认开启写设备", "High", "FAIL", "主 runner 在 run 正常结束后会进入 `_maybe_run_postrun_corrected_delivery`；默认配置把 `postrun_corrected_delivery.enabled=True`、`write_devices=True`、`verify_short_run.enabled=True` 一并打开。", [spans["runner_postrun_corrected_delivery"], spans["config_postrun_corrected_delivery"]], postrun_hook_commits, "这会把一次完成的 V1 运行自动带入后处理写回/短验证链路，扩大真实环境风险面，也会让审查主流程与后处理流程的边界变模糊。", "先把该链路从默认主流程边界里剥离成显式人工确认步骤，至少在审计/回归阶段默认关闭写回和短验证。", 0.96),
        EvidenceItem("E003", "点位导出缺少独立保存时间戳", "Medium", "FAIL", "样本行保存了 `sample_ts/sample_start_ts/sample_end_ts` 以及多种设备采样时间，但点位汇总与样本导出都没有独立的 `save_ts`/`insert_ts` 字段。", [spans["runner_collect_samples"], spans["runner_build_point_summary_row"], spans["logger_log_sample"], spans["logger_log_point"]], point_summary_commits, "审 V1 点位是否“及时落盘”时，只能看采样时间，无法区分采样发生时间和真正写盘/入库时间。", "给样本级与点位级导出同时补一个统一的保存时间戳，并明确采样时间与保存时间的语义。", 0.93),
        EvidenceItem("E004", "主流程前后系数追溯不完整", "Medium", "FAIL", "主 runner 可写系数，也可调用 corrected autodelivery，但当前主路径不自动保存写前系数快照；完整的 before/after coefficient provenance 只存在于独立 sidecar 工具链。", [spans["runner_maybe_write_coefficients"], spans["runner_postrun_corrected_delivery"], spans["corrected_build_delivery"]], sorted({*runner_coeff_commits, *postrun_hook_commits}), "当需要复核 CO2/H2O 系数是否真的闭环、是否回退到上次值、是否写入了预期组别时，主 runner 产物不足以直接证明。", "把 before/after coefficient snapshot、目标值、readback 值整合进主 runner 的 formal artifact，而不是只留在旁路工具里。", 0.88),
        EvidenceItem("E005", "点位链路具备稳态与 handoff 保护", "Low", "PASS", "采样前链路具备压力后门禁、freshness gate、route handoff deferred export 和唯一 point tag 组合，静态上可以解释为什么不会直接把切点瞬间或上一点位数据落到当前点位。", [spans["runner_wait_pressure_sampling_ready"], spans["runner_wait_sampling_freshness_gate"], spans["runner_enqueue_deferred_sample_exports"], spans["logger_log_point_samples"]], sorted({*blame_commits(spans["runner_wait_pressure_sampling_ready"]), *blame_commits(spans["runner_wait_sampling_freshness_gate"]), *blame_commits(spans["runner_enqueue_deferred_sample_exports"])}), "这部分是本轮静态审计里对“上一点位/过渡态/立即最新值被误存”的主要正向证据。", "继续保留这些保护，并在回归里长期保留只读 trace 检查。", 0.85),
        EvidenceItem("E006", "H2O 路线存在，但零点/跨度业务语义仍不明确", "Low", "UNKNOWN", "代码里能确认 H2O 路线、H2O point tag、以及 H2O coefficient groups 2/4 的存在；但没有找到与 CO2 zero/span 对等的 H2O 零点/跨度判定步骤。", [spans["runner_run_h2o_group"], spans["runner_h2o_point_tag"], spans["corrected_build_download_plan"]], sorted({*blame_commits(spans["runner_run_h2o_group"]), *blame_commits(spans["corrected_build_download_plan"])}), "后续如果要审“H2O 零点/跨度是否闭环”，当前静态证据还不足以直接落 PASS/FAIL。", "补一份明确的 H2O 点位业务定义或 acceptance 口径，再做下一轮核对。", 0.72),
    ]


def run_trace_check(spans: Dict[str, FileSpan]) -> Tuple[str, List[Tuple[str, str, str]], str]:
    cmd = [sys.executable, "-m", "pytest", "-q", TRACE_TEST]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode == 0:
        details = [
            ("点位样本字段完整性", "PASS", f"{span_group(spans['test_audit_trace_fields'], spans['runner_collect_samples'])}"),
            ("不同点位不会互相覆盖", "PASS", f"{span_group(spans['test_audit_trace_overwrite'], spans['logger_log_point_samples'], spans['logger_append_points_csv'])}"),
            ("保存前存在稳定/窗口/新鲜度门禁", "PASS", f"{span_group(spans['test_audit_trace_guards'], spans['runner_wait_pressure_sampling_ready'], spans['runner_wait_sampling_freshness_gate'])}"),
        ]
        status = "PASS"
    else:
        details = [("只读 trace 检查", "FAIL", f"pytest 返回码 `{proc.returncode}`；请查看输出。证据参考 {spans['test_audit_trace_guards'].as_ref()}。")]
        status = "FAIL"
    output = f"$ {' '.join(cmd)}\n{proc.stdout}{proc.stderr}".rstrip() + "\n"
    return status, details, output


def build_trace_check_report(spans: Dict[str, FileSpan], *, generated_at: str) -> str:
    status, details, output = run_trace_check(spans)
    lines = ["# 只读 Trace 检查", "", f"- 生成时间: {generated_at}", f"- 命令: `python -m pytest -q {TRACE_TEST}`", f"- 总结论: {status}", "", "## 结果", ""]
    for title, result, evidence in details:
        lines.append(f"- {title}: {result} | 证据: {evidence}")
    lines.extend(["", "## 原始输出", "", "```text", output.rstrip(), "```", ""])
    return "\n".join(lines)


def ensure_output_dirs(output_dir: Path) -> None:
    (output_dir / "raw").mkdir(parents=True, exist_ok=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a read-only V1 calibration audit bundle.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="Git history cutoff, default: %(default)s")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Audit output directory, default: %(default)s")
    parser.add_argument("--status-override-file", default="", help="Use an existing git status snapshot instead of live `git status`.")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
