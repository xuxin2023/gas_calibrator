"""Build a V1.5 recommendation-closure audit from code and evidence.

This module is intentionally offline-only. It never opens COM ports, controls
gas/water routes, controls valves/PACE, or writes analyzer coefficients. Its
job is to make engineering recommendations traceable: which items are already
implemented in V1.5, which are only partially closed, and which still need a
formal integration step.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "v1_5_recommendation_closure_v1"
STATUS_ORDER = {"open": 0, "partial": 1, "closed": 2}


@dataclass(frozen=True)
class ClosureEvidence:
    path: str
    present: bool
    matched_patterns: tuple[str, ...] = ()
    missing_patterns: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecommendationItem:
    recommendation_id: str
    title: str
    status: str
    physical_meaning: str
    implemented_evidence: tuple[ClosureEvidence, ...]
    tests: tuple[str, ...]
    remaining_gap: str
    next_action: str

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["implemented_evidence"] = [item.to_json() for item in self.implemented_evidence]
        return payload


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig")


def _file_evidence(repo_root: Path, relative_path: str, patterns: Sequence[str] = ()) -> ClosureEvidence:
    path = repo_root / relative_path
    text = _read_text(path)
    matched = tuple(pattern for pattern in patterns if pattern in text)
    missing = tuple(pattern for pattern in patterns if pattern not in text)
    return ClosureEvidence(
        path=str(path.resolve()),
        present=path.exists(),
        matched_patterns=matched,
        missing_patterns=missing,
    )


def _all_closed(evidence: Iterable[ClosureEvidence]) -> bool:
    rows = tuple(evidence)
    return bool(rows) and all(row.present and not row.missing_patterns for row in rows)


def _any_present(evidence: Iterable[ClosureEvidence]) -> bool:
    return any(row.present or row.matched_patterns for row in evidence)


def _status_from_evidence(
    evidence: Sequence[ClosureEvidence],
    *,
    force_partial: bool = False,
    force_open: bool = False,
) -> str:
    if force_open:
        return "open"
    if _all_closed(evidence) and not force_partial:
        return "closed"
    if _any_present(evidence):
        return "partial"
    return "open"


def _item(
    *,
    recommendation_id: str,
    title: str,
    physical_meaning: str,
    evidence: Sequence[ClosureEvidence],
    tests: Sequence[str],
    remaining_gap: str,
    next_action: str,
    force_partial: bool = False,
    force_open: bool = False,
) -> RecommendationItem:
    return RecommendationItem(
        recommendation_id=recommendation_id,
        title=title,
        status=_status_from_evidence(evidence, force_partial=force_partial, force_open=force_open),
        physical_meaning=physical_meaning,
        implemented_evidence=tuple(evidence),
        tests=tuple(tests),
        remaining_gap=remaining_gap,
        next_action=next_action,
    )


def build_v1_5_recommendation_closure(
    *,
    repo_root: str | Path,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Return a static closure model for the current V1.5 recommendation set."""

    root = Path(repo_root).resolve()
    run_path = Path(run_dir).resolve() if run_dir else None
    items: list[RecommendationItem] = []

    items.append(
        _item(
            recommendation_id="active_frame_recovery",
            title="旧缓存帧/无效帧恢复与拒绝",
            physical_meaning=(
                "采样窗口必须来自当前通气状态下的新鲜 MODE2 帧；旧缓存帧或缺帧不能静默进入拟合。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/config.py",
                    (
                        "active_frame_recovery_enabled",
                        "active_frame_recovery_wait_s",
                    ),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/workflow/runner.py",
                    (
                        "_active_analyzer_anchor_match_with_recovery",
                        "stale_frame",
                    ),
                ),
                _file_evidence(
                    root,
                    "tests/test_runner_multi_analyzers.py",
                    (
                        "test_active_analyzer_anchor_match_recovers_from_stale_frame",
                        "test_merge_analyzer_cache_rejects_unrecovered_stale_active_frame",
                    ),
                ),
            ],
            tests=("tests/test_runner_multi_analyzers.py",),
            remaining_gap="需要在后续真实运行中确认每台设备的 stale/no-data 统计进入最终证书附件。",
            next_action="保留当前恢复窗口，按设备 ID 汇总 stale/no-data 计数到正式报告。",
        )
    )

    items.append(
        _item(
            recommendation_id="status_register_qc_logic",
            title="状态寄存器三态 QC",
            physical_meaning=(
                "状态寄存器把光功率、光电流、脉冲同步、温度异常、信号超限等内部故障接入证据链。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/workflow/runner.py",
                    (
                        "_assess_status_register_qc",
                        "status_register_qc",
                    ),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/logging_utils.py",
                    ("status_register_qc",),
                ),
                _file_evidence(
                    root,
                    "tests/test_runner_multi_analyzers.py",
                    ("test_status_register_qc_distinguishes_missing_pass_and_fail",),
                ),
            ],
            tests=("tests/test_runner_multi_analyzers.py",),
            remaining_gap=(
                "本轮六台设备聚合证据里状态寄存器非空行为 0；逻辑已能判定，但还不能证明采集源已闭环。"
            ),
            next_action="把状态寄存器原始值和 bit 中文解释加入正式采样证据与报告。",
            force_partial=True,
        )
    )

    items.append(
        _item(
            recommendation_id="route_open_until_sample_end",
            title="采样必须发生在开阀开放流通状态",
            physical_meaning=(
                "CO2/H2O 主校准应采持续刷新后的标准气状态，不能先关阀再取样。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
                    ("route_open_until_sample_end", "gas_route_open_until_sample_end"),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
                    ("route_open_until_sample_end", "h2o_route_open_until_sample_end"),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_formal_open_flow.py",
                    ("route_open_until_sample_end=False",),
                ),
            ],
            tests=("tests/test_v1_5_formal_open_flow.py",),
            remaining_gap="需要持续确保队列模式、批处理模式和手工恢复模式都保留该字段。",
            next_action="把该字段纳入证书/技术报告的点位证据表。",
        )
    )

    items.append(
        _item(
            recommendation_id="per_analyzer_independent_grade",
            title="每台分析仪独立判稳与独立评级",
            physical_meaning=(
                "多台串联采样时，一台设备通信或光学异常不应污染其它设备的数据结论。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
                    (
                        "per_analyzer_ratio_stability_required",
                        "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject",
                    ),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
                    (
                        "per_analyzer_status_register_qc_required",
                        "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject",
                    ),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_formal_open_flow_sampling_runner.py",
                    (
                        "per_analyzer_ratio_stability_required",
                        "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject",
                    ),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_formal_h2o_open_flow_sampling_runner.py",
                    (
                        "per_analyzer_h2o_ratio_stability_required",
                        "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject",
                    ),
                ),
            ],
            tests=(
                "tests/test_v1_5_formal_open_flow_artifacts.py",
                "tests/test_v1_5_formal_open_flow_sampling_runner.py",
                "tests/test_v1_5_formal_h2o_open_flow_sampling_runner.py",
            ),
            remaining_gap="需要在 UI/报告中把 rejected_by_device 与 accepted_by_device 更直观地展示。",
            next_action="报告按设备 ID 输出 A/B/C/拒绝等级和拒绝原因。",
        )
    )

    items.append(
        _item(
            recommendation_id="factory_signal_health_gate",
            title="工厂模式光学信号健康门禁",
            physical_meaning=(
                "ref_signal、CO2/H2O signal、ratio/raw ratio 能区分光路/探测器/参考满值异常和普通系数误差。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/validation/factory_signal_health_review.py",
                    ("pass_factory_signal_health",),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/export_v1_5_factory_signal_health_review.py",
                    ("factory_signal_health",),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/v1_5/orchestration/full_flow.py",
                    ("factory_signal_health_review", "requires_factory_signal_health_review"),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_full_flow_orchestration.py",
                    ("test_full_flow_plan_requires_factory_signal_health_before_fit_review",),
                ),
            ],
            tests=(
                "tests/test_v1_5_factory_signal_health_review.py",
                "tests/test_v1_5_full_flow_orchestration.py",
            ),
            remaining_gap=(
                "073/079 这类光学异常仍需要把 SETCO2/SETPOW/状态寄存器读数接成同一份维修诊断证据。"
            ),
            next_action="在光学健康门禁报告里加入 SETCO2、SETPOW、状态寄存器、同点横向对比。",
        )
    )

    items.append(
        _item(
            recommendation_id="optical_root_cause_report",
            title="六台设备光学根因中文报告",
            physical_meaning=(
                "报告把同点横向比较、工厂信号、缓存帧、状态寄存器缺失和可能故障根因交给维修/研发复核。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "tools/generate_v15_optical_root_cause_docx.py",
                    ("六台气体分析仪光学根因分析增强报告",),
                ),
            ],
            tests=(),
            remaining_gap="当前是独立诊断报告生成器，尚未作为 formal archive closure 的默认步骤。",
            next_action="把 DOCX/Markdown 作为 diagnostic_analysis 角色纳入最终 evidence bundle。",
            force_partial=True,
        )
    )

    items.append(
        _item(
            recommendation_id="run_evidence_status_auto_refresh",
            title="运行证据状态索引自动刷新",
            physical_meaning=(
                "实际跑完、写完、入库、出证后，系统证据索引也必须自动反映最终状态，避免人知道完成但证据树未闭环。"
            ),
            evidence=[
                _file_evidence(
                    root,
                    "src/gas_calibrator/validation/v1_5_run_evidence_status.py",
                    ("build_v1_5_run_evidence_status",),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/validation/v1_5_formal_archive_closure.py",
                    ("build_v1_5_formal_archive_closure", "build_v1_5_run_evidence_status"),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py",
                    ("run_evidence_status_final_json", "build_v1_5_run_evidence_status"),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_formal_archive_closure.py",
                    ("has_run_evidence_status",),
                ),
                _file_evidence(
                    root,
                    "tests/test_v1_5_full_flow_orchestration.py",
                    ("full_flow_closure_readiness",),
                ),
            ],
            tests=(
                "tests/test_v1_5_run_evidence_status.py",
                "tests/test_v1_5_formal_archive_closure.py",
                "tests/test_v1_5_full_flow_orchestration.py",
            ),
            remaining_gap="",
            next_action="继续把最终证据状态索引接入 UI 和报告总览，便于审核员直接看到最终闭环状态。",
        )
    )

    normalized_items: list[RecommendationItem] = []
    for item in items:
        if item.recommendation_id == "optical_root_cause_report":
            optical_evidence = (
                _file_evidence(
                    root,
                    "tools/generate_v15_optical_root_cause_docx.py",
                    ("光学根因",),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/validation/v1_5_run_evidence_status.py",
                    ("diagnostic_analysis", "optical_root_cause"),
                ),
                _file_evidence(
                    root,
                    "src/gas_calibrator/storage/v1_5_evidence/bundle.py",
                    ("diagnostic_analysis", "optical_root_cause"),
                ),
            )
            status = _status_from_evidence(optical_evidence)
            item = RecommendationItem(
                recommendation_id=item.recommendation_id,
                title="六台设备光学根因中文报告归档",
                status=status,
                physical_meaning=(
                    "报告把同点横向比较、工厂模式信号、缓存帧、状态寄存器和可能故障根因交给维修/研发复核，"
                    "并作为 diagnostic_analysis 进入最终证据包。"
                ),
                implemented_evidence=optical_evidence,
                tests=item.tests,
                remaining_gap="" if status == "closed" else "光学根因报告尚未完整纳入 diagnostic_analysis 证据角色。",
                next_action="在最终 evidence bundle 中按 diagnostic_analysis 角色审核光学根因报告。",
            )
        normalized_items.append(item)
    items = normalized_items

    counts = {"closed": 0, "partial": 0, "open": 0}
    for item in items:
        counts[item.status] = counts.get(item.status, 0) + 1
    if counts.get("open"):
        overall = "needs_follow_up"
    elif counts.get("partial"):
        overall = "partially_closed"
    else:
        overall = "closed"

    return {
        "schema": SCHEMA,
        "created_at": _now(),
        "repo_root": str(root),
        "run_dir": str(run_path) if run_path else "",
        "overall_status": overall,
        "summary_counts": counts,
        "physical_boundaries": {
            "offline_audit_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
        "items": [item.to_json() for item in items],
        "recommended_regression_commands": [
            "python -m pytest tests/test_runner_multi_analyzers.py tests/test_v1_5_formal_open_flow.py -q",
            "python -m pytest tests/test_v1_5_factory_signal_health_review.py tests/test_v1_5_full_flow_orchestration.py -q",
            "python -m pytest tests/test_v1_5_run_evidence_status.py tests/test_v1_5_formal_archive_closure.py -q",
        ],
    }


def render_v1_5_recommendation_closure_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 建议改进闭环表",
        "",
        f"- 总体状态：`{model.get('overall_status')}`",
        f"- 代码根目录：`{model.get('repo_root')}`",
        f"- 运行目录：`{model.get('run_dir') or '未指定'}`",
        "",
        "## 物理边界",
        "",
    ]
    for key, value in (model.get("physical_boundaries") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## 汇总", ""])
    counts = model.get("summary_counts") or {}
    lines.append(f"- 已闭环：`{counts.get('closed', 0)}`")
    lines.append(f"- 部分闭环：`{counts.get('partial', 0)}`")
    lines.append(f"- 未闭环：`{counts.get('open', 0)}`")
    lines.extend(["", "## 逐项闭环", ""])
    lines.append("| 建议 | 状态 | 物理意义 | 剩余缺口 | 下一步 |")
    lines.append("| --- | --- | --- | --- | --- |")
    for item in model.get("items") or []:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item.get("title", "")),
                    f"`{item.get('status', '')}`",
                    str(item.get("physical_meaning", "")),
                    str(item.get("remaining_gap", "")),
                    str(item.get("next_action", "")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## 建议回归命令", ""])
    for command in model.get("recommended_regression_commands") or []:
        lines.append(f"- `{command}`")
    return "\n".join(lines).rstrip() + "\n"


def write_v1_5_recommendation_closure(
    *,
    repo_root: str | Path,
    output_dir: str | Path,
    run_dir: str | Path | None = None,
) -> dict[str, Path]:
    model = build_v1_5_recommendation_closure(repo_root=repo_root, run_dir=run_dir)
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_recommendation_closure.json"
    md_path = out / "v1_5_recommendation_closure.md"
    csv_path = out / "v1_5_recommendation_closure.csv"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_v1_5_recommendation_closure_markdown(model), encoding="utf-8-sig")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "recommendation_id",
                "title",
                "status",
                "physical_meaning",
                "remaining_gap",
                "next_action",
                "tests",
            ],
        )
        writer.writeheader()
        for item in model.get("items") or []:
            writer.writerow(
                {
                    "recommendation_id": item.get("recommendation_id"),
                    "title": item.get("title"),
                    "status": item.get("status"),
                    "physical_meaning": item.get("physical_meaning"),
                    "remaining_gap": item.get("remaining_gap"),
                    "next_action": item.get("next_action"),
                    "tests": ";".join(item.get("tests") or []),
                }
            )
    return {"json": json_path, "markdown": md_path, "csv": csv_path}
