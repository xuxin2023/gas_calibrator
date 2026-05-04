from __future__ import annotations

from pathlib import Path

from gas_calibrator.v2.analytics.sidecar_views import build_sidecar_analytics_summary
from gas_calibrator.v2.intelligence.review_copilot import (
    build_model_governance_summary,
    build_review_copilot_payload,
)
from gas_calibrator.v2.storage.sidecar_index import SidecarIndexStore


def _seed_store(store: SidecarIndexStore, *, run_id: str) -> None:
    store.upsert("runs", {"run_id": run_id, "status": "completed"})
    store.upsert("artifacts", {"run_id": run_id, "artifact_key": "summary.json", "path": "summary.json"})
    store.upsert("manifests", {"run_id": run_id, "manifest_key": "manifest.json", "path": "manifest.json"})
    store.upsert("reviews", {"run_id": run_id, "review_id": "review-1", "summary": "offline review"})
    store.upsert("coefficients", {"run_id": run_id, "coefficient_id": "coef-1", "version": "v1"})
    store.upsert(
        "anomaly_cases",
        {
            "run_id": run_id,
            "case_id": "case-1",
            "tag": "pressure_drift",
            "severity": "high",
            "state": "open",
            "device": "pressure_gauge",
            "window_refs": ["window:preseal"],
            "root_cause_candidates": ["gauge drift", "seal leak"],
            "reviewer_conclusion": "need replay verification",
        },
    )
    store.upsert(
        "feature_snapshots",
        {
            "run_id": run_id,
            "snapshot_id": "snapshot-1",
            "feature_version": "feature_v2026_04",
            "window_refs": ["window:preseal"],
            "signal_family": "pressure",
            "values": {"span_hpa": 0.42},
            "linked_decision_diff": "decision drift +0.12",
        },
    )
    store.upsert(
        "model_registry",
        {
            "run_id": run_id,
            "model_id": "model-1",
            "model_version": "risk-model-1.2.0",
            "feature_version": "feature_v2026_04",
            "label_version": "labels_v3",
            "evaluation_metrics": {"f1": 0.91},
            "release_status": "canary",
            "rollback_target": "risk-model-1.1.4",
            "human_review_required": True,
        },
    )
    store.upsert(
        "model_evaluations",
        {
            "run_id": run_id,
            "evaluation_id": "eval-1",
            "model_version": "risk-model-1.2.0",
            "feature_version": "feature_v2026_04",
            "label_version": "labels_v3",
            "evaluation_metrics": {"auc": 0.95},
            "release_status": "canary",
            "rollback_target": "risk-model-1.1.4",
            "human_review_required": True,
        },
    )
    store.upsert(
        "review_digests",
        {
            "run_id": run_id,
            "digest_id": "digest-1",
            "risk_summary": "high risk | pressure drift",
            "evidence_gaps": ["missing replay confirmation", "missing standards note"],
            "revalidation_suggestions": ["rerun pressure window"],
            "standards_gap_navigation": [{"standard": "ISO17025", "gap": "missing trace chain"}],
        },
    )
    store.upsert(
        "run_risk_scores",
        {
            "run_id": run_id,
            "score_id": "risk-1",
            "risk_score": 0.82,
            "risk_level": "high",
            "risk_summary": "high risk | score 0.82",
        },
    )


def test_sidecar_index_file_backend_roundtrips_and_normalizes(tmp_path: Path) -> None:
    store = SidecarIndexStore.file_backed(tmp_path / "sidecar" / "index.json")
    _seed_store(store, run_id="run-sidecar-file")

    anomaly = store.query("anomaly_cases", run_id="run-sidecar-file")[0]
    feature = store.query("feature_snapshots", run_id="run-sidecar-file")[0]

    assert anomaly["tag"] == "pressure_drift"
    assert anomaly["window_refs"] == ["window:preseal"]
    assert anomaly["root_cause_candidates"] == ["gauge drift", "seal leak"]
    assert anomaly["reviewer_only"] is True
    assert anomaly["not_real_acceptance_evidence"] is True
    assert feature["feature_version"] == "feature_v2026_04"
    assert feature["signal_family"] == "pressure"
    assert feature["linked_decision_diff"] == "decision drift +0.12"

    summary = build_sidecar_analytics_summary(store, run_id="run-sidecar-file")
    assert summary["available"] is True
    assert summary["collections"]["runs"] == 1
    assert "旁路索引" in summary["summary_line"]
    assert "pressure_drift" in summary["anomaly_taxonomy_summary"]


def test_sidecar_index_sqlite_backend_and_ai_governance_payloads(tmp_path: Path) -> None:
    store = SidecarIndexStore.sqlite_sidecar(tmp_path / "sidecar" / "index.sqlite")
    _seed_store(store, run_id="run-sidecar-sqlite")

    governance = build_model_governance_summary(store, run_id="run-sidecar-sqlite")
    copilot = build_review_copilot_payload(store, run_id="run-sidecar-sqlite")

    assert governance["model_version"] == "risk-model-1.2.0"
    assert governance["feature_version"] == "feature_v2026_04"
    assert governance["label_version"] == "labels_v3"
    assert governance["release_status"] == "canary"
    assert governance["rollback_target"] == "risk-model-1.1.4"
    assert governance["human_review_required"] is True
    assert governance["not_device_control"] is True

    assert copilot["risk_summary"] == "high risk | score 0.82"
    assert "missing replay confirmation" in copilot["evidence_gaps"]
    assert copilot["revalidation_suggestions"] == ["rerun pressure window"]
    assert copilot["standards_gap_navigation"][0]["standard"] == "ISO17025"
    assert copilot["control_actions"] == []
    assert copilot["not_coefficient_writeback"] is True


def test_sidecar_index_missing_sqlite_does_not_raise(tmp_path: Path) -> None:
    store = SidecarIndexStore.sqlite_sidecar(tmp_path / "missing" / "index.sqlite")

    assert store.query("runs", run_id="missing-run") == []
    assert build_sidecar_analytics_summary(store, run_id="missing-run") == {}
    assert build_review_copilot_payload(store, run_id="missing-run") == {}
    assert build_model_governance_summary(store, run_id="missing-run") == {}
