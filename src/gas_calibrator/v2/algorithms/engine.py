from __future__ import annotations

from dataclasses import replace
from typing import Any, Optional

from ..config import AIConfig
from ..domain.explanation_models import (
    AlgorithmRecommendation,
    PointRejection,
    Recommendation,
)
from ..intelligence.advisors import AlgorithmAdvisor
from .registry import AlgorithmRegistry
from .result_types import ComparisonResult, FitResult
from .validator import BackValidator


class AlgorithmEngine:
    """Algorithm selection and comparison engine."""

    def __init__(
        self,
        registry: AlgorithmRegistry,
        advisor: Optional[AlgorithmAdvisor] = None,
        ai_config: Optional[AIConfig] = None,
    ):
        self.registry = registry
        self.validator = BackValidator()
        self.advisor = advisor
        self.ai_config = ai_config or AIConfig()

    def fit_with(
        self,
        algorithm_name: str,
        samples,
        point_results,
        config: Optional[dict] = None,
    ) -> FitResult:
        algorithm = self.registry.get(algorithm_name, config)
        fit_result = algorithm.fit(samples, point_results)
        fit_result.algorithm_spec = algorithm.get_spec()
        fit_result.confidence = self.validator.assess_confidence(fit_result)
        fit_result.confidence_level = self.validator.get_confidence_level(fit_result.confidence)
        return fit_result

    def compare(self, algorithm_names: list[str], samples, point_results) -> ComparisonResult:
        results = {name: self.fit_with(name, samples, point_results) for name in algorithm_names}
        ranking = sorted(
            results.keys(),
            key=lambda name: (results[name].valid, results[name].r_squared, -results[name].rmse),
            reverse=True,
        )
        best = ranking[0]
        metrics = {
            name: {
                "r_squared": float(result.r_squared),
                "rmse": float(result.rmse),
                "confidence": float(result.confidence),
                "valid": 1.0 if result.valid else 0.0,
            }
            for name, result in results.items()
        }
        ai_recommendation = ""
        if self.advisor is not None and self.ai_config.feature_enabled("algorithm_recommendation"):
            try:
                ai_recommendation = self.advisor.recommend(results, self._dataset_features(samples, point_results))
            except Exception:
                ai_recommendation = ""
        return ComparisonResult(
            best_algorithm=best,
            results=results,
            ranking=ranking,
            recommendation=f"Best R²={results[best].r_squared:.4f} from {best}",
            ai_recommendation=ai_recommendation,
            metrics=metrics,
        )

    def auto_select(
        self,
        samples,
        point_results,
        candidates: Optional[list[str]] = None,
    ) -> FitResult:
        if candidates is None:
            candidates = self.registry.list_algorithms()
        comparison = self.compare(candidates, samples, point_results)
        selected = comparison.results[comparison.best_algorithm]
        if not comparison.ai_recommendation:
            return selected
        warnings = list(selected.warnings)
        warnings.append(f"AI recommendation: {comparison.ai_recommendation}")
        return replace(
            selected,
            message=str(selected.message or comparison.ai_recommendation),
            warnings=warnings,
        )

    def explain_selection(self, comparison: ComparisonResult) -> AlgorithmRecommendation:
        scores = {
            name: max(
                0.0,
                min(
                    1.0,
                    (result.r_squared * 0.7)
                    + (result.confidence * 0.2)
                    + ((1.0 / (1.0 + result.rmse)) * 0.1),
                ),
            )
            for name, result in comparison.results.items()
        }
        best = comparison.best_algorithm
        best_result = comparison.results[best]
        alternatives = [name for name in comparison.ranking if name != best]
        reason = comparison.ai_recommendation or (
            f"{best} has the strongest fit quality with R²={best_result.r_squared:.4f}, "
            f"RMSE={best_result.rmse:.4f}, confidence={best_result.confidence:.2f}"
        )
        return AlgorithmRecommendation(
            selected_algorithm=best,
            reason=reason,
            comparison_summary=comparison.recommendation,
            ranking=list(comparison.ranking),
            scores=scores,
            recommendation=Recommendation(
                action="use" if best_result.valid else "investigate",
                reason=f"Select {best} as the primary fitting algorithm",
                details=[
                    f"Top-ranked among {len(comparison.results)} candidates",
                    f"Confidence level: {best_result.confidence_level}",
                ],
                confidence=best_result.confidence,
                alternatives=alternatives,
            ),
        )

    def explain_rejection(self, point_index: int, qc_result: Any) -> PointRejection:
        reason_value = getattr(qc_result, "reason", "")
        reasons = [item for item in str(reason_value).split(",") if item] if reason_value else []
        outlier_ratio = float(getattr(qc_result, "outlier_ratio", 0.0) or 0.0)
        usable_sample_count = int(getattr(qc_result, "usable_sample_count", 0) or 0)
        quality_score = float(getattr(qc_result, "quality_score", 0.0) or 0.0)
        rejected = not bool(getattr(qc_result, "valid", False))
        recommendation_action = "exclude" if rejected and quality_score < 0.5 else ("retry" if rejected else "use")
        return PointRejection(
            point_index=point_index,
            rejected=rejected,
            reasons=(reasons or ["passed"]) if not rejected else (reasons or ["qc_failed"]),
            qc_score=quality_score,
            sample_count=usable_sample_count,
            outlier_count=int(round(outlier_ratio * max(usable_sample_count, 1))),
            recommendation=Recommendation(
                action=recommendation_action,
                reason=str(getattr(qc_result, "recommendation", "review")),
                details=reasons,
                confidence=max(0.0, min(1.0, quality_score)),
                alternatives=["re-sample point", "inspect sensor stability"] if rejected else [],
            ),
        )

    @staticmethod
    def _dataset_features(samples, point_results) -> dict[str, Any]:
        co2_values = [
            float(getattr(item, "mean_co2", 0.0))
            for item in point_results
            if getattr(item, "mean_co2", None) is not None
        ]
        h2o_values = [
            float(getattr(item, "mean_h2o", 0.0))
            for item in point_results
            if getattr(item, "mean_h2o", None) is not None
        ]
        temp_values = []
        for item in point_results:
            for key in ("temperature_c", "chamber_temp_c"):
                value = getattr(item, key, None)
                if value is not None:
                    temp_values.append(float(value))
                    break
        valid_points = sum(1 for item in point_results if bool(getattr(item, "accepted", True)))
        total_points = len(point_results)
        return {
            "point_count": total_points,
            "valid_points": valid_points,
            "quality_score": valid_points / max(1, total_points),
            "sample_count": len(samples),
            "co2_range": AlgorithmEngine._format_range(co2_values),
            "h2o_range": AlgorithmEngine._format_range(h2o_values),
            "temp_range": AlgorithmEngine._format_range(temp_values),
        }

    @staticmethod
    def _format_range(values: list[float]) -> str:
        if not values:
            return "n/a"
        return f"{min(values):.3f} ~ {max(values):.3f}"
