from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Recommendation:
    """Structured recommendation with rationale."""

    action: str = ""
    reason: str = ""
    details: list[str] = field(default_factory=list)
    confidence: float = 0.0
    alternatives: list[str] = field(default_factory=list)

    def explain(self) -> str:
        detail_text = f" Details: {'; '.join(self.details)}." if self.details else ""
        alt_text = f" Alternatives: {', '.join(self.alternatives)}." if self.alternatives else ""
        return f"Action={self.action or 'unspecified'} because {self.reason or 'no reason provided'}.{detail_text}{alt_text}"


@dataclass
class AlgorithmRecommendation:
    """Recommendation for algorithm selection."""

    selected_algorithm: str
    reason: str
    comparison_summary: str
    ranking: list[str] = field(default_factory=list)
    scores: dict[str, float] = field(default_factory=dict)
    recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        ranking_text = f" Ranking: {', '.join(self.ranking)}." if self.ranking else ""
        return f"Selected {self.selected_algorithm} because {self.reason}. {self.comparison_summary}.{ranking_text}".strip()


@dataclass
class PointRejection:
    """Explanation for rejecting or warning on a point."""

    point_index: int
    rejected: bool
    reasons: list[str] = field(default_factory=list)
    qc_score: float = 0.0
    sample_count: int = 0
    outlier_count: int = 0
    recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        state = "rejected" if self.rejected else "accepted"
        reasons = ", ".join(self.reasons) if self.reasons else "no explicit reasons"
        return f"Point {self.point_index} {state}: {reasons}. QC={self.qc_score:.2f}, samples={self.sample_count}, outliers={self.outlier_count}"


@dataclass
class RunExplanation:
    """Human-readable explanation for one run."""

    run_id: str
    total_points: int
    valid_points: int
    rejected_points: int
    algorithm_recommendation: Optional[AlgorithmRecommendation] = None
    point_rejections: list[PointRejection] = field(default_factory=list)
    overall_quality: float = 0.0
    overall_confidence: float = 0.0
    final_recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        return self.to_report()

    def to_report(self) -> str:
        lines = [
            f"Run {self.run_id}: {self.valid_points}/{self.total_points} valid points, {self.rejected_points} rejected.",
            f"Overall quality={self.overall_quality:.2f}, confidence={self.overall_confidence:.2f}.",
        ]
        if self.algorithm_recommendation is not None:
            lines.append(self.algorithm_recommendation.explain())
        for rejection in self.point_rejections:
            lines.append(rejection.explain())
        if self.final_recommendation.action or self.final_recommendation.reason:
            lines.append(self.final_recommendation.explain())
        return "\n".join(lines)
