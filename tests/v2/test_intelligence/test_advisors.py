from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.intelligence import AIRuntime, explainers as explainers_module
from gas_calibrator.v2.intelligence.advisors import AlgorithmAdvisor, AnomalyAdvisor
from gas_calibrator.v2.intelligence.explainers import FitExplainer, RunExplainer
from gas_calibrator.v2.intelligence.llm_client import LLMConfig, MockLLMClient


def test_anomaly_advisor_uses_mock_llm() -> None:
    advisor = AnomalyAdvisor(MockLLMClient(LLMConfig(provider="mock", model="mock")))

    text = advisor.diagnose(
        anomaly_type="device_error",
        phase="SAMPLING",
        device="analyzer_1",
        error_message="timeout",
        log_excerpt="serial timeout",
    )

    assert "诊断结论" in text
    assert "建议操作" in text


def test_algorithm_advisor_uses_mock_llm() -> None:
    advisor = AlgorithmAdvisor(MockLLMClient(LLMConfig(provider="mock", model="mock")))
    fit_results = {
        "linear": SimpleNamespace(r_squared=0.98, rmse=0.1, confidence=0.9),
        "polynomial": SimpleNamespace(r_squared=0.95, rmse=0.2, confidence=0.8),
    }

    text = advisor.recommend(
        fit_results,
        {
            "point_count": 6,
            "valid_points": 5,
            "quality_score": 0.84,
            "co2_range": "0~5000",
            "h2o_range": "0~1000",
            "temp_range": "20~35",
        },
    )

    assert "推荐算法" in text
    assert "算法比较" in text


def test_fit_and_run_explainers_use_mock_llm() -> None:
    llm = MockLLMClient(LLMConfig(provider="mock", model="mock"))
    fit_explainer = FitExplainer(llm)
    run_explainer = RunExplainer(llm)

    fit_text = fit_explainer.explain(
        SimpleNamespace(algorithm_name="linear", r_squared=0.98, rmse=0.1, mae=0.05, confidence=0.9),
        [
            SimpleNamespace(mean_co2=100.0, mean_h2o=20.0, temperature_c=25.0, accepted=True),
            SimpleNamespace(mean_co2=200.0, mean_h2o=25.0, temperature_c=25.5, accepted=True),
        ],
    )
    run_text = run_explainer.explain(
        SimpleNamespace(run_id="run_001", total_points=2),
        SimpleNamespace(algorithm_name="linear", r_squared=0.98, rmse=0.1),
        SimpleNamespace(overall_score=0.85, valid_points=2),
    )

    assert fit_text.startswith("[Mock Response]")
    assert run_text.startswith("[Mock Response]")


def test_advisors_have_one_package_owner() -> None:
    expected_module = "gas_calibrator.v2.intelligence.advisors"
    assert AlgorithmAdvisor.__module__ == expected_module
    assert AnomalyAdvisor.__module__ == expected_module


def test_ai_runtime_has_one_package_owner() -> None:
    assert AIRuntime.__module__ == "gas_calibrator.v2.intelligence"


def test_explainers_have_one_package_owner() -> None:
    expected_module = "gas_calibrator.v2.intelligence.explainers"
    assert FitExplainer.__module__ == expected_module
    assert RunExplainer.__module__ == expected_module


def test_explainer_prompt_assets_have_one_explicit_inventory() -> None:
    prompt_dir = Path(explainers_module.__file__).resolve().parent.parent / "prompts"
    assert sorted(path.name for path in prompt_dir.glob("*.txt")) == [
        "algorithm_recommend.txt",
        "report_summary.txt",
    ]
