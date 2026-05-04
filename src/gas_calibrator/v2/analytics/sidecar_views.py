from __future__ import annotations

from typing import Any

from ..storage.sidecar_index import SIDECAR_COLLECTIONS, SidecarIndexStore


def _text(value: Any, default: str = "--") -> str:
    text = str(value or "").strip()
    return text or default


def build_sidecar_analytics_summary(
    sidecar_index: SidecarIndexStore | None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    if sidecar_index is None or not getattr(sidecar_index, "enabled", False):
        return {}
    try:
        counts = sidecar_index.collection_counts(run_id=run_id)
        anomalies = sidecar_index.query("anomaly_cases", run_id=run_id, limit=6)
        feature_snapshots = sidecar_index.query("feature_snapshots", run_id=run_id, limit=6)
        reviews = sidecar_index.query("reviews", run_id=run_id, limit=4)
        digests = sidecar_index.query("review_digests", run_id=run_id, limit=3)
        risk_rows = sidecar_index.query("run_risk_scores", run_id=run_id, limit=3)
    except Exception as exc:
        return {
            "available": False,
            "backend": getattr(sidecar_index, "backend", "unknown"),
            "index_path": str(getattr(sidecar_index, "path", "")),
            "error": str(exc),
            "summary_line": f"sidecar index unavailable | {exc}",
        }

    if not any(counts.values()):
        return {}

    first_anomaly = dict(anomalies[0]) if anomalies else {}
    first_snapshot = dict(feature_snapshots[0]) if feature_snapshots else {}
    first_risk = dict(risk_rows[0]) if risk_rows else {}
    present_collections = [name for name in SIDECAR_COLLECTIONS if counts.get(name)]
    summary_line = (
        f"旁路索引 {sidecar_index.backend} | runs {counts.get('runs', 0)} | artifacts {counts.get('artifacts', 0)} "
        f"| reviews {counts.get('reviews', 0)} | anomalies {counts.get('anomaly_cases', 0)} "
        f"| features {counts.get('feature_snapshots', 0)} | risk {counts.get('run_risk_scores', 0)}"
    )
    anomaly_line = (
        f"异常分类: {_text(first_anomaly.get('tag'))} | {_text(first_anomaly.get('severity'))} "
        f"| {_text(first_anomaly.get('state'))} | {_text(first_anomaly.get('device'))}"
        if first_anomaly
        else "异常分类: --"
    )
    feature_line = (
        f"特征快照: {_text(first_snapshot.get('feature_version'))} | {_text(first_snapshot.get('signal_family'))} "
        f"| {_text(first_snapshot.get('linked_decision_diff'))}"
        if first_snapshot
        else "特征快照: --"
    )
    risk_line = (
        f"风险评分: {_text(first_risk.get('risk_level'))} | {float(first_risk.get('risk_score', 0.0) or 0.0):.2f}"
        if first_risk
        else "风险评分: --"
    )
    boundary_line = "旁路索引仅供 analytics/review 导航，不进入 file-artifact-first 默认主链"
    return {
        "available": True,
        "backend": sidecar_index.backend,
        "index_path": str(sidecar_index.path),
        "run_id": str(run_id or ""),
        "collections": dict(counts),
        "collections_present": present_collections,
        "summary_line": summary_line,
        "summary_lines": [summary_line, anomaly_line, feature_line, risk_line, boundary_line],
        "compact_summary_lines": [summary_line, anomaly_line, feature_line, risk_line],
        "anomaly_taxonomy_summary": anomaly_line,
        "feature_snapshot_summary": feature_line,
        "risk_summary_line": risk_line,
        "artifact_review_summary": (
            f"artifact/manifests/reviews: {counts.get('artifacts', 0)}/{counts.get('manifests', 0)}/{counts.get('reviews', 0)}"
        ),
        "recent_anomaly_cases": anomalies,
        "recent_feature_snapshots": feature_snapshots,
        "recent_reviews": reviews,
        "recent_review_digests": digests,
        "recent_run_risk_scores": risk_rows,
        "reviewer_only": True,
        "advisory_only": True,
        "sidecar_only": True,
        "file_artifact_first_preserved": True,
        "main_chain_dependency": False,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "not_device_control": True,
        "not_sampling_release": True,
        "not_coefficient_writeback": True,
        "not_formal_metrology_conclusion": True,
    }
