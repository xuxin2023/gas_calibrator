import csv
import json
from datetime import datetime, timedelta

from gas_calibrator.v2.algorithms.engine import AlgorithmEngine
from gas_calibrator.v2.algorithms.registry import AlgorithmRegistry
from gas_calibrator.v2.config import AIConfig, AIFeaturesConfig
from gas_calibrator.v2.domain.result_models import PointResult
from gas_calibrator.v2.domain.sample_models import RawSample
from gas_calibrator.v2.intelligence.advisors import AlgorithmAdvisor, AnomalyAdvisor
from gas_calibrator.v2.intelligence.llm_client import LLMConfig, MockLLMClient
from gas_calibrator.v2.intelligence.summarizer import Summarizer


def _ai_config() -> AIConfig:
    return AIConfig(
        enabled=True,
        provider="mock",
        features=AIFeaturesConfig(
            run_summary=True,
            qc_explanation=True,
            anomaly_diagnosis=True,
            algorithm_recommendation=True,
        ),
    )


def test_summarizer_writes_artifacts(tmp_path) -> None:
    run_dir = tmp_path / "run_20260320_043540"
    run_dir.mkdir()
    summary_payload = {
        "run_id": run_dir.name,
        "status": {"phase": "completed", "total_points": 2},
        "stats": {
            "point_summaries": [
                {
                    "point": {"index": 1, "route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0},
                    "stats": {"valid": True, "reason": "passed", "total_time_s": 120.0},
                },
                {
                    "point": {"index": 2, "route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0},
                    "stats": {"valid": False, "reason": "humidity_stability_timeout", "total_time_s": 180.0},
                },
            ]
        },
    }
    qc_payload = {
        "run_id": run_dir.name,
        "total_points": 2,
        "valid_points": 1,
        "invalid_points": 1,
        "overall_score": 0.7963,
        "grade": "B",
        "recommendations": ["检查湿度发生器。"],
    }
    with (run_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary_payload, fh, ensure_ascii=False, indent=2)
    with (run_dir / "qc_report.json").open("w", encoding="utf-8") as fh:
        json.dump(qc_payload, fh, ensure_ascii=False, indent=2)
    with (run_dir / "samples.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["timestamp", "analyzer_id"])
        writer.writeheader()
        writer.writerow({"timestamp": "2026-03-20T04:35:40", "analyzer_id": "ga01"})
        writer.writerow({"timestamp": "2026-03-20T04:35:41", "analyzer_id": "ga01"})

    summarizer = Summarizer(MockLLMClient(LLMConfig(provider="mock", model="mock")), _ai_config())
    text = summarizer.write_summary(run_dir, anomaly_diagnosis="诊断结论：湿度发生器响应慢。\n\n建议操作：\n1. 检查湿度发生器。")

    assert "整体质量评分" in text
    assert (run_dir / "run_summary.txt").exists()
    updated_summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert "ai_summary" in updated_summary
    assert "建议：" in updated_summary["ai_summary"]


def test_anomaly_advisor_generates_run_diagnosis() -> None:
    advisor = AnomalyAdvisor(MockLLMClient(LLMConfig(provider="mock", model="mock")), _ai_config())

    text = advisor.diagnose_run(
        failed_points=[
            {"point_index": 1, "route": "h2o", "reason": "humidity_stability_timeout"},
            {"point_index": 2, "route": "h2o", "reason": "humidity_stability_timeout"},
            {"point_index": 3, "route": "h2o", "reason": "humidity_stability_timeout"},
        ],
        device_events=[{"device": "humidity_generator"}, {"device": "humidity_generator"}],
        alarms=[{"severity": "warning", "category": "humidity"}],
    )

    assert "诊断结论" in text
    assert "建议操作" in text
    assert "湿度" in text or "H2O" in text


def test_algorithm_engine_records_ai_recommendation() -> None:
    registry = AlgorithmRegistry()
    registry.register_default_algorithms()
    ai_config = _ai_config()
    engine = AlgorithmEngine(
        registry,
        advisor=AlgorithmAdvisor(MockLLMClient(LLMConfig(provider="mock", model="mock")), ai_config),
        ai_config=ai_config,
    )
    point_results = [
        PointResult(point_index=1, mean_co2=0.0, mean_h2o=0.0, sample_count=3, stable=True),
        PointResult(point_index=2, mean_co2=1.0, mean_h2o=2.0, sample_count=3, stable=True),
        PointResult(point_index=3, mean_co2=2.0, mean_h2o=4.0, sample_count=3, stable=True),
        PointResult(point_index=4, mean_co2=3.0, mean_h2o=6.0, sample_count=3, stable=True),
    ]
    base = datetime(2026, 3, 18, 13, 0, 0)
    samples = [
        RawSample(
            timestamp=base + timedelta(seconds=index),
            point_index=index + 1,
            analyzer_name="ga01",
            co2=float(index),
            h2o=float(index * 2),
        )
        for index in range(4)
    ]

    comparison = engine.compare(["linear", "polynomial"], samples, point_results)
    recommendation = engine.explain_selection(comparison)
    selected = engine.auto_select(samples, point_results, ["linear", "polynomial"])

    assert comparison.ai_recommendation
    assert "推荐算法" in comparison.ai_recommendation
    assert recommendation.reason
    assert selected.message
