from __future__ import annotations

from typing import Any

from ..storage.sidecar_index import SidecarIndexStore


def _text(value: Any, default: str = "--") -> str:
    text = str(value or "").strip()
    return text or default


def build_review_copilot_payload(
    sidecar_index: SidecarIndexStore | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    if sidecar_index is None or not getattr(sidecar_index, "enabled", False):
        return {}
    try:
        digests = sidecar_index.query("review_digests", run_id=run_id, limit=3)
        risks = sidecar_index.query("run_risk_scores", run_id=run_id, limit=3)
        anomalies = sidecar_index.query("anomaly_cases", run_id=run_id, limit=6)
        feature_snapshots = sidecar_index.query("feature_snapshots", run_id=run_id, limit=3)
        model_rows = sidecar_index.query("model_registry", run_id=run_id, limit=3)
    except Exception as exc:
        return {
            "available": False,
            "summary_line": f"Review Copilot unavailable | {exc}",
            "error": str(exc),
        }

    latest_digest = dict(digests[0]) if digests else {}
    latest_risk = dict(risks[0]) if risks else {}
    evidence_gaps = [str(item) for item in list(latest_digest.get("evidence_gaps") or []) if str(item).strip()]
    if not evidence_gaps and not feature_snapshots:
        evidence_gaps.append("missing feature snapshots")
    if not evidence_gaps and not model_rows:
        evidence_gaps.append("missing model registry")
    revalidation = [
        str(item)
        for item in list(latest_digest.get("revalidation_suggestions") or [])[:4]
        if str(item).strip()
    ]
    if not revalidation:
        for anomaly in anomalies[:2]:
            root_causes = [str(item) for item in list(dict(anomaly).get("root_cause_candidates") or []) if str(item).strip()]
            if root_causes:
                revalidation.append(f"复验 {dict(anomaly).get('device') or 'device'} | {root_causes[0]}")
    standards = [
        dict(item)
        for item in list(latest_digest.get("standards_gap_navigation") or [])[:4]
        if isinstance(item, dict)
    ]
    risk_summary = _text(
        latest_risk.get("risk_summary")
        or latest_digest.get("risk_summary")
        or latest_risk.get("risk_level")
    )
    if risk_summary == "--" and anomalies:
        risk_summary = f"{len(anomalies)} unresolved anomalies"
    if risk_summary == "--" and not (digests or risks or anomalies):
        return {}
    summary_line = (
        f"Review Copilot | {risk_summary} | 证据缺口 {len(evidence_gaps)} "
        f"| 复验建议 {len(revalidation)} | standards {len(standards)}"
    )
    standards_line = (
        "standards gap: "
        + " | ".join(
            f"{_text(item.get('standard'))}:{_text(item.get('gap'))}"
            for item in standards
        )
        if standards
        else "standards gap: --"
    )
    boundary_line = "Copilot 仅提供审阅导航，不触发任何控制动作或正式放行结论"
    return {
        "available": True,
        "run_id": str(run_id or ""),
        "risk_summary": risk_summary,
        "evidence_gaps": evidence_gaps,
        "revalidation_suggestions": revalidation,
        "standards_gap_navigation": standards,
        "summary_line": summary_line,
        "summary_lines": [
            summary_line,
            f"风险摘要: {risk_summary}",
            f"证据缺口: {' | '.join(evidence_gaps) if evidence_gaps else '--'}",
            f"复验建议: {' | '.join(revalidation) if revalidation else '--'}",
            standards_line,
            boundary_line,
        ],
        "compact_summary_lines": [
            summary_line,
            f"证据缺口 {len(evidence_gaps)}",
            f"复验建议 {len(revalidation)}",
            f"standards {len(standards)}",
        ],
        "control_actions": [],
        "control_action_blockers": [
            "not_device_control",
            "not_sampling_release",
            "not_coefficient_writeback",
            "not_formal_metrology_conclusion",
        ],
        "reviewer_only": True,
        "advisory_only": True,
        "sidecar_only": True,
        "human_review_required": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "not_device_control": True,
        "not_sampling_release": True,
        "not_coefficient_writeback": True,
        "not_formal_metrology_conclusion": True,
    }


def build_model_governance_summary(
    sidecar_index: SidecarIndexStore | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    if sidecar_index is None or not getattr(sidecar_index, "enabled", False):
        return {}
    try:
        model_rows = sidecar_index.query("model_registry", run_id=run_id, limit=6)
        eval_rows = sidecar_index.query("model_evaluations", run_id=run_id, limit=6)
    except Exception as exc:
        return {
            "available": False,
            "summary_line": f"model governance unavailable | {exc}",
            "error": str(exc),
        }

    if not model_rows and not eval_rows:
        return {}

    active_model = dict(model_rows[0]) if model_rows else {}
    active_eval = next(
        (
            dict(item)
            for item in eval_rows
            if _text(dict(item).get("model_version")) == _text(active_model.get("model_version"))
        ),
        dict(eval_rows[0]) if eval_rows else {},
    )
    model_version = _text(active_model.get("model_version"))
    feature_version = _text(active_model.get("feature_version") or active_eval.get("feature_version"))
    label_version = _text(active_model.get("label_version") or active_eval.get("label_version"))
    evaluation_metrics = dict(active_eval.get("evaluation_metrics") or active_model.get("evaluation_metrics") or {})
    release_status = _text(active_model.get("release_status") or active_eval.get("release_status"))
    rollback_target = _text(active_model.get("rollback_target") or active_eval.get("rollback_target"))
    human_review_required = bool(
        active_model.get("human_review_required", active_eval.get("human_review_required", True))
    )
    summary_line = (
        f"model {model_version} | feature {feature_version} | label {label_version} "
        f"| release {release_status} | rollback {rollback_target}"
    )
    human_review_line = f"human review required: {'yes' if human_review_required else 'no'}"
    metrics_line = (
        "evaluation metrics: "
        + " | ".join(f"{key}={value}" for key, value in evaluation_metrics.items())
        if evaluation_metrics
        else "evaluation metrics: --"
    )
    return {
        "available": True,
        "run_id": str(run_id or ""),
        "model_version": model_version,
        "feature_version": feature_version,
        "label_version": label_version,
        "evaluation_metrics": evaluation_metrics,
        "release_status": release_status,
        "rollback_target": rollback_target,
        "human_review_required": human_review_required,
        "summary_line": summary_line,
        "summary_lines": [summary_line, metrics_line, human_review_line],
        "compact_summary_lines": [summary_line, metrics_line, human_review_line],
        "reviewer_only": True,
        "advisory_only": True,
        "sidecar_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "not_device_control": True,
        "not_sampling_release": True,
        "not_coefficient_writeback": True,
        "not_formal_metrology_conclusion": True,
    }
