from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import csv
import json
from pathlib import Path
from typing import Any, Optional

from ..core.models import CalibrationPoint
from .point_validator import PointValidationResult
from .quality_scorer import RunQualityScore


@dataclass(frozen=True)
class QCReport:
    run_id: str
    timestamp: datetime
    total_points: int
    valid_points: int
    invalid_points: int
    overall_score: float
    grade: str
    point_details: list[dict] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    decision_counts: dict[str, int] = field(default_factory=dict)
    run_gate: dict[str, Any] = field(default_factory=dict)
    point_gate_summary: dict[str, Any] = field(default_factory=dict)
    route_decision_breakdown: dict[str, dict[str, int]] = field(default_factory=dict)
    reject_reason_taxonomy: list[dict[str, Any]] = field(default_factory=list)
    failed_check_taxonomy: list[dict[str, Any]] = field(default_factory=list)
    rule_profile: dict[str, Any] = field(default_factory=dict)
    threshold_profile: dict[str, Any] = field(default_factory=dict)
    reviewer_digest: dict[str, Any] = field(default_factory=dict)
    reviewer_card: dict[str, Any] = field(default_factory=dict)
    review_sections: list[dict[str, Any]] = field(default_factory=list)
    evidence_boundary: dict[str, Any] = field(default_factory=dict)


class QCReporter:
    """Generates QC report artifacts."""

    def __init__(self, run_id: str = ""):
        self.run_id = run_id

    def generate(
        self,
        all_results: list[tuple[CalibrationPoint, PointValidationResult]],
        run_score: RunQualityScore,
        *,
        rule_profile: Optional[dict[str, Any]] = None,
        threshold_profile: Optional[dict[str, Any]] = None,
        evidence_boundary: Optional[dict[str, Any]] = None,
    ) -> QCReport:
        point_details = []
        valid_points = 0
        for point, validation in all_results:
            if validation.valid:
                valid_points += 1
            reason_codes = _reason_codes(getattr(validation, "reason", ""))
            result_level = _result_level(validation)
            point_details.append(
                {
                    "point_index": point.index,
                    "route": point.route,
                    "temperature_c": point.temperature_c,
                    "co2_ppm": point.co2_ppm,
                    "quality_score": validation.quality_score,
                    "valid": validation.valid,
                    "result_level": result_level,
                    "recommendation": validation.recommendation,
                    "reason": validation.reason,
                    "reason_codes": reason_codes,
                    "reject_categories": [_reject_category(code) for code in reason_codes],
                    "failed_checks": list(getattr(validation, "failed_checks", []) or []),
                    "ai_explanation": str(getattr(validation, "ai_explanation", "") or ""),
                }
            )
        total_points = len(point_details)
        invalid_points = total_points - valid_points
        summary_payload = build_qc_review_payload(
            point_rows=point_details,
            run_id=self.run_id or "unknown",
            overall_score=float(getattr(run_score, "overall_score", 0.0) or 0.0),
            grade=str(getattr(run_score, "grade", "--") or "--"),
            recommendations=list(getattr(run_score, "recommendations", []) or []),
        )
        return QCReport(
            run_id=self.run_id or "unknown",
            timestamp=datetime.now(),
            total_points=total_points,
            valid_points=valid_points,
            invalid_points=invalid_points,
            overall_score=run_score.overall_score,
            grade=run_score.grade,
            point_details=point_details,
            recommendations=list(run_score.recommendations),
            decision_counts=dict(summary_payload.get("decision_counts", {}) or {}),
            run_gate=dict(summary_payload.get("run_gate", {}) or {}),
            point_gate_summary=dict(summary_payload.get("point_gate_summary", {}) or {}),
            route_decision_breakdown=dict(summary_payload.get("route_decision_breakdown", {}) or {}),
            reject_reason_taxonomy=list(summary_payload.get("reject_reason_taxonomy", []) or []),
            failed_check_taxonomy=list(summary_payload.get("failed_check_taxonomy", []) or []),
            rule_profile=dict(rule_profile or {}),
            threshold_profile=dict(threshold_profile or {}),
            reviewer_digest=dict(summary_payload.get("reviewer_digest", {}) or {}),
            reviewer_card=dict(summary_payload.get("reviewer_card", {}) or {}),
            review_sections=[dict(item) for item in list(summary_payload.get("review_sections", []) or [])],
            evidence_boundary=dict(evidence_boundary or {}),
        )

    def export_json(self, report: QCReport, path: Path) -> None:
        payload = asdict(report)
        payload["timestamp"] = report.timestamp.isoformat(timespec="seconds")
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def export_summary_json(self, report: QCReport, path: Path) -> None:
        evidence_section = build_qc_evidence_section(
            reviewer_digest=dict(report.reviewer_digest or {}),
            reviewer_card=dict(report.reviewer_card or {}),
            run_gate=dict(report.run_gate or {}),
            point_gate_summary=dict(report.point_gate_summary or {}),
            decision_counts=dict(report.decision_counts or {}),
            route_decision_breakdown=dict(report.route_decision_breakdown or {}),
            reject_reason_taxonomy=list(report.reject_reason_taxonomy or []),
            failed_check_taxonomy=list(report.failed_check_taxonomy or []),
            review_sections=[dict(item) for item in list(report.review_sections or []) if isinstance(item, dict)],
            evidence_source=str(dict(report.evidence_boundary or {}).get("evidence_source") or "simulated_protocol"),
            evidence_state=str(dict(report.evidence_boundary or {}).get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(dict(report.evidence_boundary or {}).get("not_real_acceptance_evidence", True)),
            acceptance_level=str(dict(report.evidence_boundary or {}).get("acceptance_level") or "offline_regression"),
            promotion_state=str(dict(report.evidence_boundary or {}).get("promotion_state") or "dry_run_only"),
        )
        payload = {
            "schema_version": "1.0",
            "artifact_type": "qc_summary",
            "run_id": report.run_id,
            "generated_at": report.timestamp.isoformat(timespec="seconds"),
            **dict(report.evidence_boundary or {}),
            "overall_score": report.overall_score,
            "grade": report.grade,
            "total_points": report.total_points,
            "valid_points": report.valid_points,
            "invalid_points": report.invalid_points,
            "decision_counts": dict(report.decision_counts or {}),
            "run_gate": dict(report.run_gate or {}),
            "point_gate_summary": dict(report.point_gate_summary or {}),
            "route_decision_breakdown": dict(report.route_decision_breakdown or {}),
            "reject_reason_taxonomy": list(report.reject_reason_taxonomy or []),
            "failed_check_taxonomy": list(report.failed_check_taxonomy or []),
            "rule_profile": dict(report.rule_profile or {}),
            "threshold_profile": dict(report.threshold_profile or {}),
            "recommendations": list(report.recommendations or []),
            "reviewer_digest": dict(report.reviewer_digest or {}),
            "reviewer_card": dict(report.reviewer_card or {}),
            "review_card_lines": list(evidence_section.get("review_card_lines") or []),
            "review_sections": [dict(item) for item in list(report.review_sections or [])],
            "evidence_section": evidence_section,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def export_manifest_json(
        self,
        report: QCReport,
        path: Path,
        *,
        report_json_path: Path,
        report_csv_path: Path,
        summary_path: Path,
        reviewer_digest_path: Path,
    ) -> None:
        evidence_section = build_qc_evidence_section(
            reviewer_digest=dict(report.reviewer_digest or {}),
            reviewer_card=dict(report.reviewer_card or {}),
            run_gate=dict(report.run_gate or {}),
            point_gate_summary=dict(report.point_gate_summary or {}),
            decision_counts=dict(report.decision_counts or {}),
            route_decision_breakdown=dict(report.route_decision_breakdown or {}),
            reject_reason_taxonomy=list(report.reject_reason_taxonomy or []),
            failed_check_taxonomy=list(report.failed_check_taxonomy or []),
            review_sections=[dict(item) for item in list(report.review_sections or []) if isinstance(item, dict)],
            evidence_source=str(dict(report.evidence_boundary or {}).get("evidence_source") or "simulated_protocol"),
            evidence_state=str(dict(report.evidence_boundary or {}).get("evidence_state") or "collected"),
            not_real_acceptance_evidence=bool(dict(report.evidence_boundary or {}).get("not_real_acceptance_evidence", True)),
            acceptance_level=str(dict(report.evidence_boundary or {}).get("acceptance_level") or "offline_regression"),
            promotion_state=str(dict(report.evidence_boundary or {}).get("promotion_state") or "dry_run_only"),
        )
        payload = {
            "schema_version": "1.0",
            "artifact_type": "qc_manifest",
            "run_id": report.run_id,
            "generated_at": report.timestamp.isoformat(timespec="seconds"),
            **dict(report.evidence_boundary or {}),
            "artifacts": [
                {"name": "qc_report_json", "path": str(report_json_path), "present": report_json_path.exists()},
                {"name": "qc_report_csv", "path": str(report_csv_path), "present": report_csv_path.exists()},
                {"name": "qc_summary_json", "path": str(summary_path), "present": summary_path.exists()},
                {"name": "qc_reviewer_digest_md", "path": str(reviewer_digest_path), "present": reviewer_digest_path.exists()},
            ],
            "decision_counts": dict(report.decision_counts or {}),
            "run_gate": dict(report.run_gate or {}),
            "point_gate_summary": dict(report.point_gate_summary or {}),
            "route_decision_breakdown": dict(report.route_decision_breakdown or {}),
            "failed_check_taxonomy": list(report.failed_check_taxonomy or []),
            "reviewer_card": dict(report.reviewer_card or {}),
            "review_card_lines": list(evidence_section.get("review_card_lines") or []),
            "evidence_section": evidence_section,
            "rule_profile": dict(report.rule_profile or {}),
            "threshold_profile": dict(report.threshold_profile or {}),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def export_reviewer_digest_markdown(self, report: QCReport, path: Path) -> None:
        digest = dict(report.reviewer_digest or {})
        reviewer_card = dict(report.reviewer_card or {})
        lines = [
            "# 质控复核摘要",
            "",
            str(digest.get("summary") or "暂无质控摘要"),
            "",
            "## 运行门禁",
            f"- 状态: {str(dict(report.run_gate or {}).get('status') or '--')}",
            f"- 说明: {str(dict(report.run_gate or {}).get('reason') or '--')}",
        ]
        point_gate = dict(report.point_gate_summary or {})
        if point_gate:
            flagged_routes = ", ".join(str(item) for item in list(point_gate.get("flagged_routes") or []) if str(item).strip()) or "none"
            lines.extend(
                [
                    "",
                    "## 点级门禁",
                    f"- 状态: {str(point_gate.get('status') or '--')}",
                    f"- 关注路由: {flagged_routes}",
                    f"- 非通过点数: {int(point_gate.get('flagged_point_count', 0) or 0)}",
                ]
            )
        route_breakdown = dict(report.route_decision_breakdown or {})
        if route_breakdown:
            lines.extend(["", "## 气路分布"])
            for route, counts in sorted(route_breakdown.items()):
                counts_payload = dict(counts or {})
                lines.append(
                    f"- {route}: pass={int(counts_payload.get('pass', 0) or 0)}, "
                    f"warn={int(counts_payload.get('warn', 0) or 0)}, "
                    f"reject={int(counts_payload.get('reject', 0) or 0)}, "
                    f"skipped={int(counts_payload.get('skipped', 0) or 0)}"
                )
        if report.reject_reason_taxonomy:
            lines.extend(["", "## 拒绝原因分类"])
            lines.extend(
                f"- {str(item.get('code') or '--')} ({str(item.get('category') or 'other')}): {int(item.get('count', 0) or 0)}"
                for item in list(report.reject_reason_taxonomy or [])
            )
        if report.failed_check_taxonomy:
            lines.extend(["", "## 失败检查分类"])
            lines.extend(
                f"- {str(item.get('code') or '--')} ({str(item.get('category') or 'other')}): {int(item.get('count', 0) or 0)}"
                for item in list(report.failed_check_taxonomy or [])
            )
        if report.recommendations:
            lines.extend(["", "## 建议"])
            lines.extend(f"- {str(item)}" for item in list(report.recommendations or []) if str(item).strip())
        reviewer_sections = [dict(item) for item in list(report.review_sections or []) if isinstance(item, dict)]
        if reviewer_sections:
            for section in reviewer_sections:
                section_title = str(section.get("title") or "").strip()
                section_lines = [str(item).strip() for item in list(section.get("lines") or []) if str(item).strip()]
                if not section_title or not section_lines:
                    continue
                if section_title in {"运行门禁", "点级门禁", "拒绝原因分类", "失败检查分类", "证据边界"}:
                    continue
                lines.extend(["", f"## {section_title}"])
                lines.extend(f"- {item}" for item in section_lines)
        elif reviewer_card:
            reviewer_lines = [str(item).strip() for item in list(reviewer_card.get("lines") or []) if str(item).strip()]
            if reviewer_lines:
                lines.extend(["", "## 审阅卡片"])
                lines.extend(f"- {item}" for item in reviewer_lines)
        evidence_source = str(dict(report.evidence_boundary or {}).get("evidence_source") or "--")
        lines.extend(
            [
                "",
                "## 证据边界",
                f"- evidence_source: {evidence_source}",
                "- 仅限 simulation/offline 复核，不代表 real acceptance evidence",
            ]
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def export_csv(self, report: QCReport, path: Path) -> None:
        fieldnames = [
            "point_index",
            "route",
            "temperature_c",
            "co2_ppm",
            "quality_score",
            "valid",
            "result_level",
            "recommendation",
            "reason",
            "reason_codes",
            "reject_categories",
            "failed_checks",
            "ai_explanation",
        ]
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            rows = []
            for detail in report.point_details:
                row = {name: detail.get(name, "") for name in fieldnames}
                for key in ("reason_codes", "reject_categories", "failed_checks"):
                    value = row.get(key, "")
                    if isinstance(value, (list, dict)):
                        row[key] = json.dumps(value, ensure_ascii=False)
                rows.append(row)
            writer.writerows(rows)


def build_qc_review_payload(
    *,
    point_rows: list[dict[str, Any]],
    run_id: str,
    overall_score: Optional[float] = None,
    grade: Optional[str] = None,
    recommendations: Optional[list[str]] = None,
) -> dict[str, Any]:
    normalized_rows = [_normalize_point_detail(item) for item in list(point_rows or [])]
    decision_counts = _decision_counts(normalized_rows)
    reject_reason_taxonomy = _reject_reason_taxonomy(normalized_rows)
    route_decision_breakdown = _route_decision_breakdown(normalized_rows)
    failed_check_taxonomy = _failed_check_taxonomy(normalized_rows)
    point_gate_summary = _point_gate_summary(normalized_rows, decision_counts=decision_counts, route_decision_breakdown=route_decision_breakdown)
    run_gate = _run_gate(
        total_points=len(normalized_rows),
        overall_score=overall_score,
        decision_counts=decision_counts,
    )
    reviewer_digest = _reviewer_digest(
        run_id=run_id,
        overall_score=overall_score,
        grade=str(grade or "--") or "--",
        decision_counts=decision_counts,
        run_gate=run_gate,
        point_gate_summary=point_gate_summary,
        route_decision_breakdown=route_decision_breakdown,
        reject_reason_taxonomy=reject_reason_taxonomy,
        failed_check_taxonomy=failed_check_taxonomy,
        recommendations=list(recommendations or []),
    )
    reviewer_card = build_qc_reviewer_card(
        reviewer_digest=reviewer_digest,
        run_gate=run_gate,
        point_gate_summary=point_gate_summary,
        decision_counts=decision_counts,
        route_decision_breakdown=route_decision_breakdown,
        reject_reason_taxonomy=reject_reason_taxonomy,
        failed_check_taxonomy=failed_check_taxonomy,
    )
    evidence_section = build_qc_evidence_section(
        reviewer_digest=reviewer_digest,
        reviewer_card=reviewer_card,
        run_gate=run_gate,
        point_gate_summary=point_gate_summary,
        decision_counts=decision_counts,
        route_decision_breakdown=route_decision_breakdown,
        reject_reason_taxonomy=reject_reason_taxonomy,
        failed_check_taxonomy=failed_check_taxonomy,
    )
    return {
        "point_rows": normalized_rows,
        "decision_counts": decision_counts,
        "run_gate": run_gate,
        "point_gate_summary": point_gate_summary,
        "route_decision_breakdown": route_decision_breakdown,
        "reject_reason_taxonomy": reject_reason_taxonomy,
        "failed_check_taxonomy": failed_check_taxonomy,
        "reviewer_digest": reviewer_digest,
        "reviewer_card": reviewer_card,
        "review_card_lines": list(evidence_section.get("review_card_lines") or []),
        "review_sections": [dict(item) for item in list(reviewer_card.get("sections") or []) if isinstance(item, dict)],
        "evidence_section": evidence_section,
    }


def _reason_codes(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw or raw == "passed":
        return []
    return [item for item in dict.fromkeys(part.strip() for part in raw.split(",") if part.strip())]


def _reject_category(code: str) -> str:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return "other"
    if "sample_count" in normalized or "missing_count" in normalized:
        return "sample_count"
    if "outlier" in normalized:
        return "outlier"
    if "signal" in normalized:
        return "signal"
    if "pressure" in normalized:
        return "pressure"
    if "temperature" in normalized:
        return "temperature"
    if "humidity" in normalized or "dew" in normalized:
        return "humidity"
    if "communication" in normalized:
        return "communication"
    if "time" in normalized:
        return "timing"
    if "quality" in normalized:
        return "quality"
    return "other"


def _normalize_point_detail(detail: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(detail or {})
    reason = str(normalized.get("reason") or "").strip() or "passed"
    recommendation = str(normalized.get("recommendation") or "").strip()
    try:
        quality_score = float(normalized.get("quality_score", 0.0) or 0.0)
    except Exception:
        quality_score = 0.0
    valid_raw = normalized.get("valid")
    valid = bool(valid_raw) if valid_raw is not None else reason == "passed"
    failed_checks = list(normalized.get("failed_checks") or [])
    reason_codes = list(normalized.get("reason_codes") or _reason_codes(reason))
    reject_categories = list(normalized.get("reject_categories") or [_reject_category(code) for code in reason_codes])
    result_level = str(normalized.get("result_level") or "").strip().lower()
    if result_level not in {"pass", "warn", "reject", "skipped"}:
        result_level = _result_level_from_values(
            valid=valid,
            recommendation=recommendation,
            quality_score=quality_score,
            reason=reason,
        )
    normalized.update(
        {
            "reason": reason,
            "recommendation": recommendation,
            "quality_score": quality_score,
            "valid": valid,
            "failed_checks": failed_checks,
            "reason_codes": reason_codes,
            "reject_categories": reject_categories,
            "result_level": result_level,
            "route": str(normalized.get("route") or "--"),
        }
    )
    return normalized


def _result_level(validation: PointValidationResult) -> str:
    return _result_level_from_values(
        valid=bool(getattr(validation, "valid", False)),
        recommendation=str(getattr(validation, "recommendation", "") or ""),
        quality_score=float(getattr(validation, "quality_score", 0.0) or 0.0),
        reason=str(getattr(validation, "reason", "") or ""),
    )


def _result_level_from_values(
    *,
    valid: bool,
    recommendation: str,
    quality_score: float,
    reason: str,
) -> str:
    normalized_reason = str(reason or "").strip().lower()
    normalized_recommendation = str(recommendation or "").strip().lower()
    if normalized_recommendation == "skip" or "skip" in normalized_reason:
        return "skipped"
    if bool(valid):
        return "pass"
    if normalized_recommendation == "review" or float(quality_score or 0.0) >= 0.5:
        return "warn"
    return "reject"


def _decision_counts(point_details: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"pass": 0, "warn": 0, "reject": 0, "skipped": 0}
    for detail in list(point_details or []):
        level = str(detail.get("result_level") or "reject").strip().lower()
        if level not in counts:
            level = "reject"
        counts[level] += 1
    return counts


def _run_gate(
    *,
    total_points: int,
    overall_score: Optional[float],
    decision_counts: dict[str, int],
) -> dict[str, Any]:
    if total_points <= 0:
        return {"status": "skipped", "reason": "no_points", "passed": False}
    resolved_score = None if overall_score is None else float(overall_score or 0.0)
    if int(decision_counts.get("reject", 0) or 0) > 0 or (resolved_score is not None and resolved_score < 0.6):
        return {"status": "reject", "reason": "reject_points_present", "passed": False}
    if int(decision_counts.get("warn", 0) or 0) > 0 or (resolved_score is not None and resolved_score < 0.8):
        return {"status": "warn", "reason": "review_required", "passed": False}
    return {"status": "pass", "reason": "ready_for_offline_review", "passed": True}


def _point_gate_summary(
    point_details: list[dict[str, Any]],
    *,
    decision_counts: dict[str, int],
    route_decision_breakdown: dict[str, dict[str, int]],
) -> dict[str, Any]:
    total_points = len(list(point_details or []))
    if total_points <= 0:
        return {"status": "skipped", "flagged_point_count": 0, "flagged_routes": [], "evaluated_routes": [], "passed": False}
    status = "pass"
    if int(decision_counts.get("reject", 0) or 0) > 0:
        status = "reject"
    elif int(decision_counts.get("warn", 0) or 0) > 0:
        status = "warn"
    elif int(decision_counts.get("skipped", 0) or 0) > 0:
        status = "skipped"
    flagged_routes = [
        route
        for route, counts in sorted(route_decision_breakdown.items())
        if int(dict(counts or {}).get("warn", 0) or 0) > 0
        or int(dict(counts or {}).get("reject", 0) or 0) > 0
        or int(dict(counts or {}).get("skipped", 0) or 0) > 0
    ]
    flagged_point_count = sum(
        int(decision_counts.get(name, 0) or 0)
        for name in ("warn", "reject", "skipped")
    )
    return {
        "status": status,
        "flagged_point_count": flagged_point_count,
        "flagged_routes": flagged_routes,
        "evaluated_routes": sorted(route_decision_breakdown),
        "passed": status == "pass",
    }


def _route_decision_breakdown(point_details: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    rows: dict[str, dict[str, int]] = {}
    for detail in list(point_details or []):
        route = str(detail.get("route") or "--").strip() or "--"
        counts = rows.setdefault(route, {"pass": 0, "warn": 0, "reject": 0, "skipped": 0})
        level = str(detail.get("result_level") or "reject").strip().lower()
        if level not in counts:
            level = "reject"
        counts[level] += 1
    return {route: dict(rows[route]) for route in sorted(rows)}


def _reject_reason_taxonomy(point_details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = {}
    for detail in list(point_details or []):
        for code in list(detail.get("reason_codes") or []):
            category = _reject_category(code)
            key = (str(code), category)
            counts[key] = counts.get(key, 0) + 1
    rows = [
        {"code": code, "category": category, "count": count}
        for (code, category), count in counts.items()
    ]
    rows.sort(key=lambda item: (-int(item["count"]), str(item["category"]), str(item["code"])))
    return rows


def _failed_check_taxonomy(point_details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = {}
    for detail in list(point_details or []):
        for item in list(detail.get("failed_checks") or []):
            if isinstance(item, dict):
                code = str(item.get("rule_name") or item.get("code") or item.get("message") or "").strip()
            else:
                code = str(item or "").strip()
            if not code:
                continue
            category = _reject_category(code)
            key = (code, category)
            counts[key] = counts.get(key, 0) + 1
    rows = [
        {"code": code, "category": category, "count": count}
        for (code, category), count in counts.items()
    ]
    rows.sort(key=lambda item: (-int(item["count"]), str(item["category"]), str(item["code"])))
    return rows


def _reviewer_digest(
    *,
    run_id: str,
    overall_score: Optional[float],
    grade: str,
    decision_counts: dict[str, int],
    run_gate: dict[str, Any],
    point_gate_summary: dict[str, Any],
    route_decision_breakdown: dict[str, dict[str, int]],
    reject_reason_taxonomy: list[dict[str, Any]],
    failed_check_taxonomy: list[dict[str, Any]],
    recommendations: list[str],
) -> dict[str, Any]:
    top_reasons = ", ".join(str(item.get("code") or "--") for item in list(reject_reason_taxonomy or [])[:3]) or "none"
    top_checks = ", ".join(str(item.get("code") or "--") for item in list(failed_check_taxonomy or [])[:3]) or "none"
    flagged_routes = ", ".join(str(item) for item in list(point_gate_summary.get("flagged_routes") or []) if str(item).strip()) or "none"
    route_summary = "; ".join(
        f"{route}: {int(dict(counts or {}).get('pass', 0) or 0)}/"
        f"{int(dict(counts or {}).get('warn', 0) or 0)}/"
        f"{int(dict(counts or {}).get('reject', 0) or 0)}/"
        f"{int(dict(counts or {}).get('skipped', 0) or 0)}"
        for route, counts in list(route_decision_breakdown.items())[:3]
    ) or "none"
    score_text = "--" if overall_score is None else f"{float(overall_score):.2f}"
    summary = (
        f"运行 {run_id} 质控评分 {score_text} / 等级 {grade}；"
        f"通过 {int(decision_counts.get('pass', 0) or 0)}，"
        f"预警 {int(decision_counts.get('warn', 0) or 0)}，"
        f"拒绝 {int(decision_counts.get('reject', 0) or 0)}，"
        f"跳过 {int(decision_counts.get('skipped', 0) or 0)}；"
        f"门禁 {str(run_gate.get('status') or '--')}。"
    )
    lines = [
        summary,
        f"点级门禁: {str(point_gate_summary.get('status') or '--')} | 关注路由: {flagged_routes}",
        f"按气路分布: {route_summary}",
        f"主要拒绝原因: {top_reasons}",
        f"失败检查: {top_checks}",
        *[str(item) for item in list(recommendations or []) if str(item).strip()],
    ]
    return {
        "summary": summary,
        "status": str(run_gate.get("status") or "--"),
        "top_reasons": top_reasons,
        "top_checks": top_checks,
        "route_summary": route_summary,
        "lines": lines,
    }


def build_qc_reviewer_card(
    *,
    reviewer_digest: dict[str, Any] | None,
    run_gate: dict[str, Any] | None,
    point_gate_summary: dict[str, Any] | None,
    decision_counts: dict[str, int] | None,
    route_decision_breakdown: dict[str, dict[str, int]] | None,
    reject_reason_taxonomy: list[dict[str, Any]] | None,
    failed_check_taxonomy: list[dict[str, Any]] | None,
    boundary_note: str = "证据边界: 仅供 simulation/offline/headless 审阅，不代表 real acceptance evidence。",
) -> dict[str, Any]:
    digest = dict(reviewer_digest or {})
    run_gate_payload = dict(run_gate or {})
    point_gate = dict(point_gate_summary or {})
    counts = dict(decision_counts or {})
    route_breakdown = dict(route_decision_breakdown or {})
    reject_taxonomy = [dict(item) for item in list(reject_reason_taxonomy or []) if isinstance(item, dict)]
    failed_taxonomy = [dict(item) for item in list(failed_check_taxonomy or []) if isinstance(item, dict)]
    summary = str(digest.get("summary") or "暂无质控摘要").strip() or "暂无质控摘要"
    explicit_lines = [str(item).strip() for item in list(digest.get("lines") or []) if str(item).strip()]
    flagged_routes = ", ".join(str(item) for item in list(point_gate.get("flagged_routes") or []) if str(item).strip()) or "none"
    route_lines = [
        (
            f"路由分布 {route}: 通过 {int(dict(route_counts or {}).get('pass', 0) or 0)} / "
            f"预警 {int(dict(route_counts or {}).get('warn', 0) or 0)} / "
            f"拒绝 {int(dict(route_counts or {}).get('reject', 0) or 0)} / "
            f"跳过 {int(dict(route_counts or {}).get('skipped', 0) or 0)}"
        )
        for route, route_counts in sorted(route_breakdown.items())
    ]
    top_reject_reason = str((reject_taxonomy[0] or {}).get("code") or "--") if reject_taxonomy else "--"
    top_failed_check = str((failed_taxonomy[0] or {}).get("code") or "--") if failed_taxonomy else "--"
    reject_taxonomy_summary = ", ".join(
        f"{str(item.get('code') or '--')}({int(item.get('count', 0) or 0)})" for item in reject_taxonomy[:3]
    ) or "--"
    failed_taxonomy_summary = ", ".join(
        f"{str(item.get('code') or '--')}({int(item.get('count', 0) or 0)})" for item in failed_taxonomy[:3]
    ) or "--"
    run_gate_line = (
        f"运行门禁: {str(run_gate_payload.get('status') or '--')} | "
        f"原因: {str(run_gate_payload.get('reason') or '--')}"
    )
    point_gate_line = (
        f"点级门禁: {str(point_gate.get('status') or '--')} | "
        f"关注路由: {flagged_routes} | "
        f"非通过点数: {int(point_gate.get('flagged_point_count', 0) or 0)}"
    )
    decision_line = (
        f"结果分级: 通过 {int(counts.get('pass', 0) or 0)} / "
        f"预警 {int(counts.get('warn', 0) or 0)} / "
        f"拒绝 {int(counts.get('reject', 0) or 0)} / "
        f"跳过 {int(counts.get('skipped', 0) or 0)}"
    )
    taxonomy_line = f"拒绝原因分类: {reject_taxonomy_summary} | 失败检查分类: {failed_taxonomy_summary}"
    top_issue_line = f"主要拒绝原因: {top_reject_reason} | 失败检查: {top_failed_check}"
    lines = [
        f"质控摘要: {summary}",
        run_gate_line,
        point_gate_line,
        decision_line,
        top_issue_line,
        *route_lines[:2],
        taxonomy_line,
    ]
    for line in explicit_lines[:3]:
        if line not in lines:
            lines.append(f"审阅结论: {line}")
    if boundary_note not in lines:
        lines.append(boundary_note)
    sections = [
        {
            "id": "summary",
            "title": "审阅结论",
            "lines": [summary, *[line for line in explicit_lines[:2] if line and line != summary]],
        },
        {
            "id": "gates",
            "title": "门禁状态",
            "lines": [run_gate_line, point_gate_line, decision_line],
        },
        {
            "id": "taxonomy",
            "title": "风险归类",
            "lines": [top_issue_line, *route_lines[:2], taxonomy_line],
        },
        {
            "id": "boundary",
            "title": "证据边界",
            "lines": [boundary_note],
        },
    ]
    return {
        "title": "质控审阅卡",
        "summary": summary,
        "status": str(run_gate_payload.get("status") or digest.get("status") or "--"),
        "lines": [str(item).strip() for item in lines if str(item).strip()],
        "sections": [
            {
                "id": str(item.get("id") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "lines": [str(line).strip() for line in list(item.get("lines") or []) if str(line).strip()],
            }
            for item in sections
            if str(item.get("id") or "").strip() and str(item.get("title") or "").strip()
        ],
    }


def build_qc_evidence_section(
    *,
    reviewer_digest: dict[str, Any] | None,
    reviewer_card: dict[str, Any] | None,
    run_gate: dict[str, Any] | None,
    point_gate_summary: dict[str, Any] | None,
    decision_counts: dict[str, int] | None,
    route_decision_breakdown: dict[str, dict[str, int]] | None,
    reject_reason_taxonomy: list[dict[str, Any]] | None,
    failed_check_taxonomy: list[dict[str, Any]] | None,
    review_sections: list[dict[str, Any]] | None = None,
    summary_override: str | None = None,
    lines_override: list[str] | None = None,
    title: str = "\u8d28\u63a7\u8bc1\u636e",
    boundary_note: str = "\u8bc1\u636e\u8fb9\u754c: \u4ec5\u4f9b simulation/offline/headless \u5ba1\u9605\uff0c\u4e0d\u4ee3\u8868 real acceptance evidence\u3002",
    evidence_source: str = "simulated_protocol",
    evidence_state: str = "offline_review",
    not_real_acceptance_evidence: bool = True,
    acceptance_level: str = "offline_regression",
    promotion_state: str = "dry_run_only",
) -> dict[str, Any]:
    digest = dict(reviewer_digest or {})
    run_gate_payload = dict(run_gate or {})
    point_gate = dict(point_gate_summary or {})
    counts = {str(key): int(value or 0) for key, value in dict(decision_counts or {}).items()}
    route_breakdown = {
        str(route): {
            "pass": int(dict(route_counts or {}).get("pass", 0) or 0),
            "warn": int(dict(route_counts or {}).get("warn", 0) or 0),
            "reject": int(dict(route_counts or {}).get("reject", 0) or 0),
            "skipped": int(dict(route_counts or {}).get("skipped", 0) or 0),
        }
        for route, route_counts in dict(route_decision_breakdown or {}).items()
        if str(route).strip()
    }
    reject_taxonomy = [dict(item) for item in list(reject_reason_taxonomy or []) if isinstance(item, dict)]
    failed_taxonomy = [dict(item) for item in list(failed_check_taxonomy or []) if isinstance(item, dict)]
    card = dict(reviewer_card or {})
    if not card or not [str(item).strip() for item in list(card.get("lines") or []) if str(item).strip()]:
        card = build_qc_reviewer_card(
            reviewer_digest=digest,
            run_gate=run_gate_payload,
            point_gate_summary=point_gate,
            decision_counts=counts,
            route_decision_breakdown=route_breakdown,
            reject_reason_taxonomy=reject_taxonomy,
            failed_check_taxonomy=failed_taxonomy,
            boundary_note=boundary_note,
        )
    summary = str(summary_override or card.get("summary") or digest.get("summary") or "\u6682\u65e0\u8d28\u63a7\u6458\u8981").strip()
    if not summary:
        summary = "\u6682\u65e0\u8d28\u63a7\u6458\u8981"
    review_card_lines = [str(item).strip() for item in list(card.get("lines") or []) if str(item).strip()]
    lines = [str(item).strip() for item in list(lines_override or []) if str(item).strip()] or list(review_card_lines)
    if not lines:
        lines = [f"\u8d28\u63a7\u6458\u8981: {summary}"]
    if boundary_note not in lines:
        lines.append(boundary_note)
    sections = [
        {
            "id": str(item.get("id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "lines": [str(line).strip() for line in list(item.get("lines") or []) if str(line).strip()],
        }
        for item in list(review_sections or card.get("sections") or [])
        if isinstance(item, dict) and str(item.get("id") or "").strip() and str(item.get("title") or "").strip()
    ]
    if not sections:
        sections = [
            {
                "id": "summary",
                "title": "\u5ba1\u9605\u7ed3\u8bba",
                "lines": list(lines[:3]),
            },
            {
                "id": "boundary",
                "title": "\u8bc1\u636e\u8fb9\u754c",
                "lines": [boundary_note],
            },
        ]
    cards = [
        {
            "id": str(item.get("id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "summary": str((list(item.get("lines") or []) or [summary])[0] or summary).strip() or summary,
            "lines": [str(line).strip() for line in list(item.get("lines") or []) if str(line).strip()],
        }
        for item in sections
        if str(item.get("id") or "").strip() and str(item.get("title") or "").strip()
    ]
    return {
        "title": str(title or "\u8d28\u63a7\u8bc1\u636e"),
        "summary": summary,
        "status": str(run_gate_payload.get("status") or card.get("status") or digest.get("status") or "--"),
        "lines": lines,
        "reviewer_digest": digest,
        "reviewer_card": card,
        "review_card_lines": review_card_lines or list(lines),
        "sections": sections,
        "review_sections": sections,
        "cards": cards,
        "run_gate": run_gate_payload,
        "point_gate_summary": point_gate,
        "decision_counts": counts,
        "route_decision_breakdown": route_breakdown,
        "reject_reason_taxonomy": reject_taxonomy,
        "failed_check_taxonomy": failed_taxonomy,
        "evidence_source": str(evidence_source or "simulated_protocol"),
        "evidence_state": str(evidence_state or "offline_review"),
        "not_real_acceptance_evidence": bool(not_real_acceptance_evidence),
        "acceptance_level": str(acceptance_level or "offline_regression"),
        "promotion_state": str(promotion_state or "dry_run_only"),
    }
