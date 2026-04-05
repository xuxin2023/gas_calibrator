from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from ..config import build_step2_config_governance_handoff
from ..core.acceptance_model import normalize_evidence_source
from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS
from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_offline_diagnostic_adapters
from ..ui_v2.artifact_registry_governance import build_current_run_governance
from ..ui_v2.i18n import t


class ResultsGateway:
    """Read-only access layer for run artifacts and derived result payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        output_files_provider: Optional[Callable[[], Iterable[str]]] = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.output_files_provider = output_files_provider

    def read_results_payload(self) -> dict[str, Any]:
        summary = self.load_json("summary.json")
        analytics_summary = self.load_json("analytics_summary.json")
        evidence_registry = self.load_json("evidence_registry.json")
        workbench_action_report = self.load_json("workbench_action_report.json")
        workbench_action_snapshot = self.load_json("workbench_action_snapshot.json")
        config_safety = self._read_summary_section(
            "config_safety",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        config_safety_review = self._read_summary_section(
            "config_safety_review",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        offline_diagnostic_adapter_summary = self._read_summary_section(
            "offline_diagnostic_adapter_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not offline_diagnostic_adapter_summary:
            offline_diagnostic_adapter_summary = summarize_offline_diagnostic_adapters(self.run_dir)
        point_taxonomy_summary = self._read_summary_section(
            "point_taxonomy_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not point_taxonomy_summary:
            point_taxonomy_summary = (
                build_point_taxonomy_handoff(list(summary.get("stats", {}).get("point_summaries", []) or []))
                if isinstance(summary, dict)
                else {}
            )
        artifact_role_summary = (
            dict(summary.get("stats", {}).get("artifact_role_summary", {}) or {}) if isinstance(summary, dict) else {}
        )
        workbench_evidence_summary = (
            dict(summary.get("stats", {}).get("workbench_evidence_summary", {}) or {}) if isinstance(summary, dict) else {}
        )
        evidence_source = self._resolve_current_run_evidence_source(workbench_evidence_summary, workbench_action_report)
        evidence_state = str(
            workbench_evidence_summary.get("evidence_state")
            or dict(workbench_action_report or {}).get("evidence_state")
            or "collected"
        )
        not_real_acceptance_evidence = bool(
            workbench_evidence_summary.get(
                "not_real_acceptance_evidence",
                dict(workbench_action_report or {}).get("not_real_acceptance_evidence", True),
            )
        )
        acceptance_level = str(
            workbench_evidence_summary.get("acceptance_level")
            or dict(workbench_action_report or {}).get("acceptance_level")
            or "offline_regression"
        )
        promotion_state = str(
            workbench_evidence_summary.get("promotion_state")
            or dict(workbench_action_report or {}).get("promotion_state")
            or "dry_run_only"
        )
        result_summary_text = self._build_result_summary_text(
            summary=summary,
            artifact_role_summary=artifact_role_summary,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
            offline_diagnostic_adapter_summary=offline_diagnostic_adapter_summary,
            point_taxonomy_summary=point_taxonomy_summary,
            workbench_evidence_summary=workbench_evidence_summary,
            evidence_source=evidence_source,
        )
        return {
            "summary": summary,
            "manifest": self.load_json("manifest.json"),
            "results": self.load_json("results.json"),
            "acceptance_plan": self.load_json("acceptance_plan.json"),
            "analytics_summary": analytics_summary,
            "spectral_quality_summary": self.load_json("spectral_quality_summary.json"),
            "trend_registry": self.load_json("trend_registry.json"),
            "lineage_summary": self.load_json("lineage_summary.json"),
            "evidence_registry": evidence_registry,
            "coefficient_registry": self.load_json("coefficient_registry.json"),
            "suite_summary": self.load_json("suite_summary.json"),
            "suite_analytics_summary": self.load_json("suite_analytics_summary.json"),
            "suite_acceptance_plan": self.load_json("suite_acceptance_plan.json"),
            "suite_evidence_registry": self.load_json("suite_evidence_registry.json"),
            "workbench_action_report": workbench_action_report,
            "workbench_action_snapshot": workbench_action_snapshot,
            "ai_summary_text": self.load_text("ai_run_summary.md") or self.load_text("run_summary.txt"),
            "output_files": self.list_output_files(),
            "reporting": dict(summary.get("reporting", {}) or {}) if isinstance(summary, dict) else {},
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": self._read_config_governance_handoff(config_safety, config_safety_review),
            "artifact_exports": dict(summary.get("stats", {}).get("artifact_exports", {}) or {}) if isinstance(summary, dict) else {},
            "artifact_role_summary": artifact_role_summary,
            "workbench_evidence_summary": workbench_evidence_summary,
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": point_taxonomy_summary,
            "result_summary_text": result_summary_text,
            "evidence_source": evidence_source,
            "evidence_state": evidence_state,
            "not_real_acceptance_evidence": not_real_acceptance_evidence,
            "acceptance_level": acceptance_level,
            "promotion_state": promotion_state,
        }

    def read_reports_payload(self) -> dict[str, Any]:
        payload = self.read_results_payload()
        manifest = dict(payload.get("manifest", {}) or {})
        role_catalog = dict(manifest.get("artifacts", {}) or {}).get("role_catalog", {})
        artifact_exports = dict(payload.get("artifact_exports", {}) or {})
        offline_diagnostic_adapter_summary = dict(payload.get("offline_diagnostic_adapter_summary", {}) or {})
        files = []
        seen: set[str] = set()

        def _artifact_path(value: Any) -> Path:
            candidate = Path(str(value or "").strip())
            if candidate.is_absolute():
                return candidate
            return self.run_dir / candidate

        candidate_paths = [self.run_dir / item for item in KNOWN_REPORT_ARTIFACTS]
        candidate_paths.extend(_artifact_path(item) for item in payload["output_files"] if str(item or "").strip())
        candidate_paths.extend(
            _artifact_path(item)
            for item in list(offline_diagnostic_adapter_summary.get("artifact_paths") or [])
            if str(item or "").strip()
        )
        for path in candidate_paths:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            present_on_disk = path.exists()
            governance = build_current_run_governance(
                path,
                artifact_exports=artifact_exports,
                role_catalog=role_catalog if isinstance(role_catalog, dict) else None,
                present_on_disk=present_on_disk,
            )
            files.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "present": present_on_disk,
                    "present_on_disk": present_on_disk,
                    "listed_in_current_run": True,
                    "artifact_origin": "current_run",
                    "scope_match": "all",
                    **governance,
                }
            )
        return {
            "run_dir": str(self.run_dir),
            "files": files,
            "ai_summary_text": str(payload.get("ai_summary_text", "") or ""),
            "result_summary_text": str(payload.get("result_summary_text", "") or ""),
            "output_files": list(payload["output_files"]),
            "reporting": dict(payload.get("reporting", {}) or {}),
            "config_safety": dict(payload.get("config_safety", {}) or {}),
            "config_safety_review": dict(payload.get("config_safety_review", {}) or {}),
            "config_governance_handoff": dict(payload.get("config_governance_handoff", {}) or {}),
            "artifact_exports": dict(payload.get("artifact_exports", {}) or {}),
            "artifact_role_summary": dict(payload.get("artifact_role_summary", {}) or {}),
            "workbench_evidence_summary": dict(payload.get("workbench_evidence_summary", {}) or {}),
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": dict(payload.get("point_taxonomy_summary", {}) or {}),
            "evidence_source": str(payload.get("evidence_source", "") or "simulated_protocol"),
            "evidence_state": str(payload.get("evidence_state", "") or "collected"),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", True)),
            "acceptance_level": str(payload.get("acceptance_level", "") or "offline_regression"),
            "promotion_state": str(payload.get("promotion_state", "") or "dry_run_only"),
        }

    def list_output_files(self) -> list[str]:
        files = set()
        if self.output_files_provider is not None:
            try:
                files.update(str(item) for item in self.output_files_provider() or [])
            except Exception:
                pass
        if self.run_dir.exists():
            for path in self.run_dir.iterdir():
                files.add(str(path))
        return sorted(files)

    def load_json(self, relative_name: str) -> dict[str, Any] | None:
        path = self.run_dir / relative_name
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def load_text(self, relative_name: str) -> str:
        path = self.run_dir / relative_name
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    @staticmethod
    def _read_section_from_payload(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        direct = payload.get(key)
        if isinstance(direct, dict):
            return dict(direct)
        stats = payload.get("stats")
        if not isinstance(stats, dict):
            return {}
        legacy = stats.get(key)
        return dict(legacy) if isinstance(legacy, dict) else {}

    @classmethod
    def _read_summary_section(cls, key: str, *payloads: dict[str, Any] | None) -> dict[str, Any]:
        for payload in payloads:
            section = cls._read_section_from_payload(payload, key)
            if section:
                return section
        return {}

    @classmethod
    def _read_config_governance_handoff(
        cls,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if config_safety_review:
            return build_step2_config_governance_handoff(config_safety_review)
        if config_safety:
            return build_step2_config_governance_handoff(config_safety)
        return {}

    @staticmethod
    def _build_result_summary_text(
        *,
        summary: dict[str, Any] | None,
        artifact_role_summary: dict[str, Any] | None,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        point_taxonomy_summary: dict[str, Any] | None,
        workbench_evidence_summary: dict[str, Any] | None,
        evidence_source: str,
    ) -> str:
        summary_payload = dict(summary or {})
        stats = dict(summary_payload.get("stats", {}) or {})
        role_summary = dict(artifact_role_summary or {})
        safety = dict(config_safety or {})
        safety_review = dict(config_safety_review or {})
        offline_summary = dict(offline_diagnostic_adapter_summary or {})
        taxonomy_summary = dict(point_taxonomy_summary or {})
        workbench_summary = dict(workbench_evidence_summary or {})

        role_parts: list[str] = []
        for role in ("execution_summary", "execution_rows", "diagnostic_analysis", "formal_analysis"):
            payload = dict(role_summary.get(role) or {})
            count = int(payload.get("count", 0) or 0)
            if count > 0:
                role_parts.append(f"{role} {count}")
        artifact_role_text = " | ".join(role_parts) if role_parts else "--"

        sample_count = int(stats.get("sample_count", 0) or 0)
        point_summary_count = len(list(stats.get("point_summaries", []) or []))
        lines = [
            f"结果文件: {'已生成' if isinstance(summary, dict) else '缺失'}",
            f"样本数: {sample_count}",
            f"点摘要数: {point_summary_count}",
            f"工件角色: {artifact_role_text}",
            f"配置安全: {str(safety_review.get('summary') or safety.get('summary') or '--')}",
        ]

        lines.insert(4, f"证据来源: {evidence_source}")
        if offline_summary:
            lines.append(
                "离线诊断: "
                + str(
                    offline_summary.get("summary")
                    or (
                        f"room-temp {int(offline_summary.get('room_temp_count', 0) or 0)} | "
                        f"analyzer-chain {int(offline_summary.get('analyzer_chain_count', 0) or 0)}"
                    )
                )
            )
        if str(offline_summary.get("coverage_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_coverage",
                    value=str(offline_summary.get("coverage_summary") or ""),
                    default=f"离线诊断覆盖：{str(offline_summary.get('coverage_summary') or '')}",
                )
            )
        if str(offline_summary.get("next_check_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_next_checks",
                    value=str(offline_summary.get("next_check_summary") or ""),
                    default=f"离线诊断下一步：{str(offline_summary.get('next_check_summary') or '')}",
                )
            )

        for detail_line in ResultsGateway._offline_diagnostic_detail_lines(offline_summary):
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_detail",
                    value=detail_line,
                    default=f"离线诊断补充: {detail_line}",
                )
            )
        point_taxonomy_summary = taxonomy_summary
        pressure_summary = str(taxonomy_summary.get("pressure_summary") or "").strip()
        pressure_mode_summary = str(taxonomy_summary.get("pressure_mode_summary") or "").strip()
        pressure_target_label_summary = str(taxonomy_summary.get("pressure_target_label_summary") or "").strip()
        if pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure",
                    value=pressure_summary,
                    default=f"压力语义：{pressure_summary}",
                )
            )
        if pressure_mode_summary and pressure_mode_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_mode",
                    value=pressure_mode_summary,
                    default=f"压力模式：{pressure_mode_summary}",
                )
            )
        if pressure_target_label_summary and pressure_target_label_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_target_label",
                    value=pressure_target_label_summary,
                    default=f"压力目标标签：{pressure_target_label_summary}",
                )
            )
        if str(point_taxonomy_summary.get("flush_gate_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_flush",
                    value=str(point_taxonomy_summary.get("flush_gate_summary") or ""),
                    default=f"冲洗门禁：{str(point_taxonomy_summary.get('flush_gate_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("preseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_preseal",
                    value=str(point_taxonomy_summary.get("preseal_summary") or ""),
                    default=f"前封气：{str(point_taxonomy_summary.get('preseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("postseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_postseal",
                    value=str(point_taxonomy_summary.get("postseal_summary") or ""),
                    default=f"后封气：{str(point_taxonomy_summary.get('postseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("stale_gauge_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_stale_gauge",
                    value=str(point_taxonomy_summary.get("stale_gauge_summary") or ""),
                    default=f"压力参考陈旧：{str(point_taxonomy_summary.get('stale_gauge_summary') or '')}",
                )
            )

        workbench_text = str(
            workbench_summary.get("summary_line")
            or workbench_summary.get("summary")
            or workbench_summary.get("review_summary")
            or "--"
        )
        lines.append(f"工作台诊断证据: {workbench_text}")

        return "\n".join(line for line in lines if str(line).strip())

    @staticmethod
    def _resolve_current_run_evidence_source(
        workbench_evidence_summary: dict[str, Any] | None,
        workbench_action_report: dict[str, Any] | None,
    ) -> str:
        source = (
            dict(workbench_evidence_summary or {}).get("evidence_source")
            or dict(workbench_action_report or {}).get("evidence_source")
            or "simulated_protocol"
        )
        return normalize_evidence_source(source)

    @staticmethod
    def _offline_diagnostic_detail_lines(
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        *,
        limit: int = 3,
    ) -> list[str]:
        summary = dict(offline_diagnostic_adapter_summary or {})
        lines = [
            str(item).strip()
            for item in list(summary.get("review_highlight_lines") or summary.get("detail_lines") or [])
            if str(item).strip()
        ]
        return lines[:limit]
