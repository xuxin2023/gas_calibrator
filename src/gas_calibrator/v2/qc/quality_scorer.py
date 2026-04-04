from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..config import QCConfig

from .point_validator import PointValidationResult


@dataclass(frozen=True)
class RunQualityScore:
    overall_score: float
    point_scores: dict[int, float] = field(default_factory=dict)
    phase_scores: dict[str, float] = field(default_factory=dict)
    grade: str = "F"
    summary: str = ""
    recommendations: list[str] = field(default_factory=list)


class QualityScorer:
    """Aggregates point-level QC into run-level scores."""

    def __init__(self, config: Optional[QCConfig] = None):
        self.config = config

    def score_point(self, validation: PointValidationResult) -> float:
        return max(0.0, min(1.0, float(validation.quality_score)))

    def score_phase(self, validations: list[PointValidationResult]) -> float:
        if not validations:
            return 0.0
        return sum(self.score_point(item) for item in validations) / len(validations)

    def score_run(self, all_validations: list[PointValidationResult]) -> RunQualityScore:
        point_scores = {item.point_index: self.score_point(item) for item in all_validations}
        overall_score = self.score_phase(all_validations)
        phase_scores = {"overall": overall_score}
        grade = self._grade(overall_score)
        invalid_count = sum(1 for item in all_validations if not item.valid)
        summary = f"{len(all_validations) - invalid_count}/{len(all_validations)} points valid"
        recommendations: list[str] = []
        if invalid_count:
            recommendations.append("Review invalid points before fitting.")
        if overall_score < 0.7:
            recommendations.append("Improve sampling stability and reduce outliers.")
        if not recommendations:
            recommendations.append("QC passed. Data is ready for fitting.")
        return RunQualityScore(
            overall_score=overall_score,
            point_scores=point_scores,
            phase_scores=phase_scores,
            grade=grade,
            summary=summary,
            recommendations=recommendations,
        )

    @staticmethod
    def _grade(score: float) -> str:
        if score >= 0.9:
            return "A"
        if score >= 0.8:
            return "B"
        if score >= 0.7:
            return "C"
        if score >= 0.6:
            return "D"
        return "F"
