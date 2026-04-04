from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean, pstdev
from typing import Any, Optional, Set

from ..config import QCConfig
from ..core.models import SamplingResult


@dataclass(frozen=True)
class OutlierResult:
    point_index: int = 0
    outlier_count: int = 0
    method: str = ""
    message: str = ""
    outlier_indices: Set[int] = field(default_factory=set)
    spike_indices: Set[int] = field(default_factory=set)
    step_indices: Set[int] = field(default_factory=set)
    drift_indices: Set[int] = field(default_factory=set)
    reasons: dict[int, str] = field(default_factory=dict)


class OutlierDetector:
    """Detects suspicious sample points before fitting."""

    def __init__(self, config: QCConfig):
        self.config = config
        self.z_threshold = float(config.spike_threshold)
        self.max_outlier_ratio = float(config.max_outlier_ratio)
        self.method = "z_score"

    def detect(self, samples: list[SamplingResult]) -> OutlierResult:
        values = self._representative_values(samples)
        if len(values) < 3:
            return OutlierResult(
                point_index=self._point_index(samples),
                outlier_count=0,
                method=self.method,
                message="Not enough samples for outlier detection",
            )

        global_std = pstdev(values) if len(values) > 1 else 0.0
        baseline_std = global_std if global_std > 0 else 1.0
        spike_indices: set[int] = set()
        step_indices: set[int] = set()
        drift_indices: set[int] = set()
        reasons: dict[int, str] = {}

        for index in range(1, len(values) - 1):
            neighbor_avg = (values[index - 1] + values[index + 1]) / 2.0
            if abs(values[index] - neighbor_avg) > self.z_threshold * baseline_std:
                spike_indices.add(index)
                reasons[index] = "spike"

        for index in range(1, len(values) - 2):
            delta = values[index] - values[index - 1]
            tail_avg = mean(values[index + 1 :])
            head_avg = mean(values[:index])
            if abs(head_avg - tail_avg) > self.z_threshold * baseline_std and abs(delta) > baseline_std:
                step_indices.add(index)
                reasons[index] = "step"

        slope = (values[-1] - values[0]) / max(1, len(values) - 1)
        if abs(slope) > self.config.drift_threshold:
            drift_indices = set(range(len(values)))
            for index in drift_indices:
                reasons.setdefault(index, "drift")

        if global_std > max(1e-6, self.z_threshold * max(abs(mean(values)), 1.0)):
            for index in range(len(values)):
                reasons.setdefault(index, "high_variance")

        outlier_indices = set(spike_indices) | set(step_indices) | set(drift_indices) | {
            index for index, reason in reasons.items() if reason == "high_variance"
        }
        return OutlierResult(
            point_index=self._point_index(samples),
            outlier_count=len(outlier_indices),
            method=self.method,
            message=f"Detected {len(outlier_indices)} outliers",
            outlier_indices=outlier_indices,
            spike_indices=spike_indices,
            step_indices=step_indices,
            drift_indices=drift_indices,
            reasons=reasons,
        )

    def detect_and_remove(
        self,
        samples: list[Any],
        field: str = "co2",
        z_thresh: float | None = None,
    ) -> tuple[list[Any], OutlierResult]:
        ordered_samples = list(samples)
        effective_z = float(self.z_threshold if z_thresh is None else z_thresh)
        values = [
            self._field_value(sample, field)
            for sample in ordered_samples
            if self._field_value(sample, field) is not None
        ]
        point_index = self._point_index(ordered_samples)

        if len(values) < 3:
            return list(ordered_samples), OutlierResult(
                point_index=point_index,
                outlier_count=0,
                method=self.method,
                message="Not enough samples for outlier detection",
            )

        avg = mean(values)
        std = pstdev(values)
        if std == 0:
            return list(ordered_samples), OutlierResult(
                point_index=point_index,
                outlier_count=0,
                method=self.method,
                message=f"No variance found in {field}",
            )

        cleaned: list[Any] = []
        outlier_indices: set[int] = set()
        reasons: dict[int, str] = {}
        for index, sample in enumerate(ordered_samples):
            value = self._field_value(sample, field)
            if value is None:
                cleaned.append(sample)
                continue

            z_score = abs((value - avg) / std)
            if z_score <= effective_z:
                cleaned.append(sample)
                continue

            outlier_indices.add(index)
            reasons[index] = f"{field}_z_score>{effective_z}"

        result = OutlierResult(
            point_index=point_index,
            outlier_count=len(outlier_indices),
            method=self.method,
            message=f"Removed {len(outlier_indices)} outliers from {field}",
            outlier_indices=outlier_indices,
            reasons=reasons,
        )
        return cleaned, result

    def mark_outliers(
        self,
        samples: list[SamplingResult],
        outlier_indices: Set[int],
    ) -> list[SamplingResult]:
        for index, sample in enumerate(samples):
            object.__setattr__(sample, "qc_outlier", index in outlier_indices)
        return samples

    @staticmethod
    def _representative_values(samples: list[Any]) -> list[float]:
        values: list[float] = []
        for sample in samples:
            value = OutlierDetector._sample_value(sample)
            if value is not None:
                values.append(value)
        return values

    @staticmethod
    def _sample_value(sample: Any) -> Optional[float]:
        for key in ("co2_signal", "co2", "h2o_signal", "h2o", "pressure_hpa", "pressure", "temperature_c"):
            value = getattr(sample, key, None)
            if value is not None:
                return float(value)
        return None

    @staticmethod
    def _field_value(sample: Any, field: str) -> Optional[float]:
        aliases = {
            "co2": ("co2", "co2_signal"),
            "h2o": ("h2o", "h2o_signal"),
            "pressure": ("pressure", "pressure_hpa"),
            "temperature_c": ("temperature_c",),
        }
        for key in aliases.get(field, (field,)):
            value = getattr(sample, key, None)
            if value is not None:
                return float(value)
        return None

    @staticmethod
    def _point_index(samples: list[Any]) -> int:
        if not samples:
            return 0
        sample = samples[0]
        point_index = getattr(sample, "point_index", None)
        if point_index is not None:
            return int(point_index)
        point = getattr(sample, "point", None)
        index = getattr(point, "index", None)
        return int(index) if index is not None else 0
