from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from statistics import median
from typing import Any, Iterable

from ..config import QCConfig


@dataclass(frozen=True)
class SampleQCResult:
    passed: bool
    sample_count: int
    expected_count: int
    missing_count: int
    has_comm_error: bool
    time_continuous: bool
    score: float
    issues: list[str] = field(default_factory=list)


class SampleChecker:
    """Checks basic sampling quality before algorithm entry."""

    def __init__(self, config: QCConfig):
        self.config = config
        self.min_count = int(config.min_sample_count)
        self.max_missing = 0

    def check(self, samples: list[Any], point_index: int | None = None) -> SampleQCResult:
        issues: list[str] = []
        sample_count = len(samples)
        expected_count = max(1, int(self.min_count))
        missing_count = max(0, expected_count - sample_count)
        has_comm_error = any(self._has_comm_error(sample) for sample in samples)
        time_continuous = self._is_time_continuous(samples)

        if sample_count < expected_count:
            issues.append(f"sample_count<{expected_count}")
        if missing_count > 0:
            issues.append(f"missing_count={missing_count}")
        if has_comm_error:
            issues.append("communication_error")
        if not time_continuous:
            issues.append("time_not_continuous")

        score = 1.0
        if expected_count > 0:
            score -= min(0.6, missing_count / expected_count * 0.6)
        if has_comm_error:
            score -= 0.2
        if not time_continuous:
            score -= 0.2
        score = max(0.0, min(1.0, score))

        passed = (
            missing_count <= max(0, int(self.max_missing))
            and not has_comm_error
            and time_continuous
            and score >= self.config.quality_threshold
        )
        return SampleQCResult(
            passed=passed,
            sample_count=sample_count,
            expected_count=expected_count,
            missing_count=missing_count,
            has_comm_error=has_comm_error,
            time_continuous=time_continuous,
            score=score,
            issues=issues,
        )

    @staticmethod
    def _has_comm_error(sample: Any) -> bool:
        co2 = getattr(sample, "co2_signal", getattr(sample, "co2", None))
        h2o = getattr(sample, "h2o_signal", getattr(sample, "h2o", None))
        return co2 is None and h2o is None

    def _is_time_continuous(self, samples: Iterable[Any]) -> bool:
        timestamps = [sample.timestamp for sample in samples if isinstance(sample.timestamp, datetime)]
        if len(timestamps) < 3:
            return True

        ordered = sorted(timestamps)
        gaps = [(ordered[index] - ordered[index - 1]).total_seconds() for index in range(1, len(ordered))]
        if any(gap <= 0 for gap in gaps):
            return False
        baseline = median(gaps)
        allowed_max_gap = max(1e-6, baseline * 2.5)
        return all(gap <= allowed_max_gap for gap in gaps)
