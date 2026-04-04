from __future__ import annotations

from dataclasses import replace
from typing import Any, Optional

from ..config import AIConfig, QCConfig
from ..core.models import CalibrationPoint
from ..domain.qc_models import CleanedData
from ..intelligence.explainers import QCExplainer
from .point_validator import PointValidationResult, PointValidator
from .qc_report import QCReport, QCReporter
from .quality_scorer import QualityScorer, RunQualityScore
from .outlier_detector import OutlierDetector
from .rule_registry import QCRuleRegistry
from .rule_templates import ModeType, QCRuleTemplate, RouteType
from .sample_checker import SampleChecker


class QCPipeline:
    """End-to-end QC pipeline for point and run validation."""

    def __init__(
        self,
        config: QCConfig,
        run_id: str = "",
        rule_registry: Optional[QCRuleRegistry] = None,
        qc_explainer: Optional[QCExplainer] = None,
        ai_config: Optional[AIConfig] = None,
    ):
        self.config = config
        self.rule_registry = rule_registry or QCRuleRegistry()
        self.ai_config = ai_config or AIConfig()
        self.qc_explainer = qc_explainer
        self.sample_checker = SampleChecker(config)
        self.outlier_detector = OutlierDetector(config)
        self.point_validator = PointValidator(config)
        self.quality_scorer = QualityScorer(config)
        self.reporter = QCReporter(run_id=run_id)
        self.last_cleaned: dict[int, CleanedData] = {}
        self.last_report: Optional[QCReport] = None
        self._current_rule: Optional[QCRuleTemplate] = None
        self._manual_rule = False
        self._register_custom_rules()
        self._apply_base_config()
        default_rule = getattr(self.config.rule_config, "default_rule", "default")
        if default_rule and default_rule != "default":
            self.set_rule(default_rule, manual=False)

    def set_rule(self, rule_name: str, *, manual: bool = True) -> None:
        self._current_rule = self.rule_registry.get(rule_name)
        self._manual_rule = manual
        if rule_name == "default":
            self._apply_base_config()
            return
        self._apply_rule(self._current_rule)

    def set_rule_for_route_mode(
        self,
        route: RouteType,
        mode: ModeType = ModeType.NORMAL,
    ) -> None:
        route_key = route.value
        mode_key = mode.value
        configured_name = None
        if mode is not ModeType.NORMAL:
            configured_name = self.config.rule_config.mode_rules.get(mode_key)
        if configured_name is None:
            configured_name = self.config.rule_config.route_rules.get(route_key)
        if configured_name is None and mode is ModeType.NORMAL:
            configured_name = self.config.rule_config.default_rule
        if configured_name is not None:
            self.set_rule(configured_name, manual=False)
            return
        self._current_rule = self.rule_registry.get_for_route_mode(route, mode)
        self._manual_rule = False
        self._apply_rule(self._current_rule)

    def process_point(
        self,
        point: Any,
        samples: list[Any],
        point_index: Optional[int] = None,
        *,
        return_cleaned: Optional[bool] = None,
    ) -> PointValidationResult | tuple[list[Any], PointValidationResult, float]:
        effective_point_index = point_index if point_index is not None else int(getattr(point, "index", 0))
        cleaned_samples, outliers, cleaned_data = self._clean_point_samples(samples, effective_point_index)
        sample_qc = self.sample_checker.check(cleaned_samples, point_index=effective_point_index)
        validation = self.point_validator.validate(point, samples, sample_qc, outliers)
        point_score = self.quality_scorer.score_point(validation)
        self.last_cleaned[effective_point_index] = cleaned_data
        validation = self._attach_ai_explanation(point, validation, cleaned_data)

        if return_cleaned or point_index is not None:
            return cleaned_samples, validation, point_score
        return validation

    def process_run(
        self,
        all_data: Optional[list[tuple[Any, list[Any]]]] = None,
        *,
        points: Optional[list[Any]] = None,
        all_samples: Optional[dict[int, list[Any]]] = None,
        return_cleaned: bool = False,
    ) -> (
        tuple[list[PointValidationResult], RunQualityScore, QCReport]
        | tuple[dict[int, list[Any]], list[PointValidationResult], RunQualityScore]
    ):
        pairs = self._normalize_run_input(all_data, points, all_samples)
        cleaned_all: dict[int, list[Any]] = {}
        point_results: list[tuple[CalibrationPoint, PointValidationResult]] = []
        validations: list[PointValidationResult] = []
        for point, samples in pairs:
            cleaned_samples, validation, _ = self.process_point(
                point,
                samples,
                point_index=int(getattr(point, "index", 0)),
                return_cleaned=True,
            )
            cleaned_all[int(getattr(point, "index", 0))] = cleaned_samples
            validations.append(validation)
            if isinstance(point, CalibrationPoint):
                point_results.append((point, validation))

        run_score = self.quality_scorer.score_run(validations)
        if return_cleaned or points is not None or all_samples is not None:
            return cleaned_all, validations, run_score

        report = self.reporter.generate(point_results, run_score)
        self.last_report = report
        return validations, run_score, report

    def _apply_rule(self, rule: QCRuleTemplate) -> None:
        self.sample_checker.min_count = int(rule.sample_count.min_count)
        self.sample_checker.max_missing = int(rule.sample_count.max_missing)
        self.point_validator.min_sample_count = int(rule.sample_count.min_count)
        self.outlier_detector.z_threshold = float(rule.outlier.z_threshold)
        self.outlier_detector.max_outlier_ratio = float(rule.outlier.max_outlier_ratio)
        self.outlier_detector.method = str(rule.outlier.method)
        self.point_validator.min_score = float(rule.quality.min_score)
        self.point_validator.pass_threshold = float(rule.quality.pass_threshold)
        self.point_validator.warn_threshold = float(rule.quality.warn_threshold)
        self.point_validator.reject_threshold = float(rule.quality.reject_threshold)
        self.point_validator.max_outlier_ratio = float(rule.outlier.max_outlier_ratio)
        self.point_validator.stability_rule = rule.stability

    def _apply_base_config(self) -> None:
        self.sample_checker.min_count = int(self.config.min_sample_count)
        self.sample_checker.max_missing = 0
        self.point_validator.min_sample_count = int(self.config.min_sample_count)
        self.outlier_detector.z_threshold = float(self.config.spike_threshold)
        self.outlier_detector.max_outlier_ratio = float(self.config.max_outlier_ratio)
        self.outlier_detector.method = "z_score"
        self.point_validator.min_score = float(self.config.quality_threshold)
        self.point_validator.pass_threshold = float(self.config.quality_threshold)
        self.point_validator.warn_threshold = max(0.0, self.point_validator.pass_threshold - 0.2)
        self.point_validator.reject_threshold = max(0.0, self.point_validator.pass_threshold - 0.4)
        self.point_validator.max_outlier_ratio = float(self.config.max_outlier_ratio)
        self.point_validator.stability_rule = None

    def _register_custom_rules(self) -> None:
        for item in getattr(self.config.rule_config, "custom_rules", []):
            if isinstance(item, dict):
                self.rule_registry.register(QCRuleTemplate.from_dict(item))

    def _attach_ai_explanation(
        self,
        point: Any,
        validation: PointValidationResult,
        cleaned_data: CleanedData,
    ) -> PointValidationResult:
        if validation.valid:
            return validation
        if self.qc_explainer is None:
            return validation
        if not self.ai_config.feature_enabled("qc_explanation"):
            return validation
        try:
            explanation = self.qc_explainer.explain_failure(
                int(getattr(point, "index", validation.point_index) or validation.point_index),
                validation,
                cleaned_data=cleaned_data,
                point=point,
            )
        except Exception:
            return validation
        return replace(validation, ai_explanation=str(explanation or ""))

    def _clean_point_samples(
        self,
        samples: list[Any],
        point_index: int,
    ) -> tuple[list[Any], Any, CleanedData]:
        original_samples = list(samples)
        original_indices = list(range(len(original_samples)))

        cleaned_after_co2, outlier_result_co2 = self.outlier_detector.detect_and_remove(
            original_samples,
            field="co2",
            z_thresh=self.outlier_detector.z_threshold,
        )
        kept_indices_after_co2 = [
            original_index
            for original_index in original_indices
            if original_index not in outlier_result_co2.outlier_indices
        ]
        cleaned_after_h2o, outlier_result_h2o = self.outlier_detector.detect_and_remove(
            cleaned_after_co2,
            field="h2o",
            z_thresh=self.outlier_detector.z_threshold,
        )
        h2o_original_indices = {
            kept_indices_after_co2[index]
            for index in outlier_result_h2o.outlier_indices
            if index < len(kept_indices_after_co2)
        }
        combined_indices = set(outlier_result_co2.outlier_indices) | h2o_original_indices
        reasons = dict(outlier_result_co2.reasons)
        reasons.update({index: "h2o_outlier" for index in h2o_original_indices})

        outliers = type(outlier_result_co2)(
            point_index=point_index,
            outlier_count=len(combined_indices),
            method=self.outlier_detector.method,
            message=f"Removed {len(combined_indices)} outliers",
            outlier_indices=combined_indices,
            spike_indices=set(outlier_result_co2.spike_indices),
            step_indices=set(outlier_result_co2.step_indices),
            drift_indices=set(outlier_result_co2.drift_indices),
            reasons=reasons,
        )
        cleaned_data = CleanedData(
            point_index=point_index,
            original_count=len(original_samples),
            cleaned_count=len(cleaned_after_h2o),
            removed_count=len(combined_indices),
            removed_indices=sorted(combined_indices),
            samples=list(cleaned_after_h2o),
            outlier_result=outliers,
        )
        return cleaned_after_h2o, outliers, cleaned_data

    @staticmethod
    def _normalize_run_input(
        all_data: Optional[list[tuple[Any, list[Any]]]],
        points: Optional[list[Any]],
        all_samples: Optional[dict[int, list[Any]]],
    ) -> list[tuple[Any, list[Any]]]:
        if points is not None or all_samples is not None:
            normalized_points = list(points or [])
            sample_map = dict(all_samples or {})
            return [
                (point, list(sample_map.get(int(getattr(point, "index", 0)), [])))
                for point in normalized_points
            ]
        return list(all_data or [])

    @staticmethod
    def _coerce_route(value: Any) -> RouteType:
        text = str(value or RouteType.BOTH.value).strip().lower()
        if text == RouteType.CO2.value:
            return RouteType.CO2
        if text == RouteType.H2O.value:
            return RouteType.H2O
        return RouteType.BOTH

    @staticmethod
    def _coerce_mode(value: Any) -> ModeType:
        text = str(value or ModeType.NORMAL.value).strip().lower()
        for item in ModeType:
            if item.value == text:
                return item
        return ModeType.NORMAL
