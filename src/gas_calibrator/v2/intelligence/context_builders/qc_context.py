from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class QCContext:
    """Normalized QC explanation payload."""

    point_index: int
    qc_score: float
    passed: bool
    reject_reasons: list[str] = field(default_factory=list)
    sample_count: int = 0
    outlier_count: int = 0
    action: str = "接受"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_reasons(validation_result: Any) -> list[str]:
    reasons = getattr(validation_result, "reasons", None)
    if isinstance(reasons, list):
        return [str(item) for item in reasons]
    reason = getattr(validation_result, "reason", "")
    if not reason:
        return []
    return [item.strip() for item in str(reason).split(",") if item.strip()]


def build_qc_context(
    point_index: int,
    validation_result: Any,
    cleaned_data: Any = None,
) -> QCContext:
    """Build a normalized context for QC explanation."""

    passed = bool(getattr(validation_result, "valid", getattr(validation_result, "passed", True)))
    score = float(getattr(validation_result, "quality_score", getattr(validation_result, "score", 0.0)) or 0.0)
    reasons = _normalize_reasons(validation_result)
    sample_count = int(getattr(validation_result, "usable_sample_count", getattr(validation_result, "sample_count", 0)) or 0)
    outlier_count = int(getattr(validation_result, "outlier_count", 0) or 0)

    if cleaned_data is not None:
        sample_count = int(getattr(cleaned_data, "cleaned_count", sample_count) or sample_count)
        outlier_count = int(getattr(cleaned_data, "removed_count", outlier_count) or outlier_count)

    return QCContext(
        point_index=point_index,
        qc_score=score,
        passed=passed,
        reject_reasons=reasons,
        sample_count=sample_count,
        outlier_count=outlier_count,
        action="接受" if passed else "剔除",
    )
