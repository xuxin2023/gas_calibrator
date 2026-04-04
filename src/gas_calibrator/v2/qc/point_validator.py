from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean
from typing import Any, Optional

from ..config import QCConfig
from ..core.models import CalibrationPoint
from .outlier_detector import OutlierResult
from .sample_checker import SampleQCResult


@dataclass(frozen=True)
class PointValidationResult:
    valid: bool
    point_index: int
    usable_sample_count: int
    outlier_ratio: float
    quality_score: float
    recommendation: str
    reason: str
    failed_checks: list[dict[str, Any]] = field(default_factory=list)
    ai_explanation: str = ""


class PointValidator:
    """Determines whether a calibration point is good enough for fitting."""

    def __init__(self, config: QCConfig):
        self.config = config
        self.min_sample_count = int(config.min_sample_count)
        self.min_score = float(config.quality_threshold)
        self.pass_threshold = float(config.quality_threshold)
        self.warn_threshold = max(0.0, self.pass_threshold - 0.2)
        self.reject_threshold = max(0.0, self.pass_threshold - 0.4)
        self.max_outlier_ratio = float(config.max_outlier_ratio)
        self.stability_rule: Any = None

    def validate(
        self,
        point: CalibrationPoint,
        samples: list[Any],
        qc_result: SampleQCResult,
        outlier_result: OutlierResult,
    ) -> PointValidationResult:
        sample_count = len(samples)
        outlier_count = len(outlier_result.outlier_indices)
        usable_sample_count = max(0, sample_count - outlier_count)
        outlier_ratio = 0.0 if sample_count == 0 else outlier_count / sample_count
        signal_span = self._signal_span(samples, outlier_result.outlier_indices)
        signal_mean = self._signal_mean(samples)
        signal_threshold = None if signal_span is None else max(1.0, abs(signal_mean) * 0.5)

        score = qc_result.score
        if sample_count > 0:
            score -= outlier_ratio * 0.5
        if signal_span is not None and signal_span > 0:
            score -= min(0.2, signal_span / max(10.0, abs(signal_span) + 1.0))
        score = max(0.0, min(1.0, score))

        reasons: list[str] = []
        failed_checks: list[dict[str, Any]] = []
        if usable_sample_count < self.min_sample_count:
            reasons.append("usable_sample_count_insufficient")
            failed_checks.append(
                self._failed_check(
                    rule_name="usable_sample_count",
                    actual=usable_sample_count,
                    threshold=self.min_sample_count,
                    message="Usable sample count is below the minimum requirement",
                )
            )
        if outlier_ratio > self.max_outlier_ratio:
            reasons.append("outlier_ratio_too_high")
            failed_checks.append(
                self._failed_check(
                    rule_name="outlier_ratio",
                    actual=outlier_ratio,
                    threshold=self.max_outlier_ratio,
                    message="Outlier ratio is above the configured limit",
                )
            )
        if not qc_result.passed:
            reasons.extend(qc_result.issues or ["sample_qc_failed"])
            failed_checks.extend(self._qc_result_checks(qc_result))
        if signal_span is not None and signal_threshold is not None and signal_span > signal_threshold:
            reasons.append("signal_span_too_wide")
            failed_checks.append(
                self._failed_check(
                    rule_name="signal_span",
                    actual=signal_span,
                    threshold=signal_threshold,
                    message="Signal span is wider than the stable range",
                )
            )

        valid = (
            usable_sample_count >= self.min_sample_count
            and outlier_ratio <= self.max_outlier_ratio
            and score >= self.min_score
            and qc_result.passed
        )
        recommendation = "use" if valid else ("review" if score >= self.warn_threshold else "exclude")
        reason = ",".join(dict.fromkeys(reasons)) if reasons else "passed"
        return PointValidationResult(
            valid=valid,
            point_index=point.index,
            usable_sample_count=usable_sample_count,
            outlier_ratio=outlier_ratio,
            quality_score=score,
            recommendation=recommendation,
            reason=reason,
            failed_checks=failed_checks,
        )

    def _qc_result_checks(self, qc_result: SampleQCResult) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        for issue in qc_result.issues or []:
            text = str(issue or "").strip()
            if not text:
                continue
            if text.startswith("sample_count<"):
                threshold = int(text.split("<", 1)[1])
                checks.append(
                    self._failed_check(
                        rule_name="sample_count",
                        actual=qc_result.sample_count,
                        threshold=threshold,
                        message="Collected sample count is lower than expected",
                    )
                )
                continue
            if text.startswith("missing_count="):
                actual = int(text.split("=", 1)[1])
                checks.append(
                    self._failed_check(
                        rule_name="missing_count",
                        actual=actual,
                        threshold=self.sample_checker_max_missing(),
                        message="Sample stream contains missing records",
                    )
                )
                continue
            if text == "communication_error":
                checks.append(
                    self._failed_check(
                        rule_name="communication_error",
                        actual=1,
                        threshold=0,
                        message="Communication errors were detected in the sample batch",
                    )
                )
                continue
            if text == "time_not_continuous":
                checks.append(
                    self._failed_check(
                        rule_name="time_continuity",
                        actual=0,
                        threshold=1,
                        message="Timestamps are not continuous",
                    )
                )
                continue
            checks.append(self._failed_check(rule_name=text, actual=None, threshold=None, message=text))
        return checks

    def sample_checker_max_missing(self) -> int:
        return 0

    @staticmethod
    def _failed_check(
        *,
        rule_name: str,
        actual: Any,
        threshold: Any,
        message: str,
    ) -> dict[str, Any]:
        return {
            "rule_name": str(rule_name),
            "actual": actual,
            "threshold": threshold,
            "message": str(message),
        }

    @staticmethod
    def _signal_span(samples: list[Any], excluded_indices: set[int]) -> Optional[float]:
        usable = [
            PointValidator._sample_value(sample)
            for index, sample in enumerate(samples)
            if index not in excluded_indices
        ]
        values = [value for value in usable if value is not None]
        if not values:
            return None
        return max(values) - min(values)

    @staticmethod
    def _signal_mean(samples: list[Any]) -> float:
        values = [PointValidator._sample_value(sample) for sample in samples]
        filtered = [value for value in values if value is not None]
        if not filtered:
            return 0.0
        return mean(filtered)

    @staticmethod
    def _sample_value(sample: Any) -> Optional[float]:
        for key in ("co2_signal", "co2", "h2o_signal", "h2o", "pressure_hpa", "pressure", "temperature_c"):
            value = getattr(sample, key, None)
            if value is not None:
                return float(value)
        return None
