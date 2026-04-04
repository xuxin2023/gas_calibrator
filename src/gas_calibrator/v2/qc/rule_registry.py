from __future__ import annotations

from .rule_templates import (
    ModeType,
    OutlierRule,
    QCRuleTemplate,
    RouteType,
    SampleCountRule,
    StabilityRule,
)


class QCRuleRegistry:
    """Registry for reusable QC rule templates."""

    def __init__(self):
        self._templates: dict[str, QCRuleTemplate] = {}
        self._register_defaults()

    def register(self, template: QCRuleTemplate) -> None:
        self._templates[template.name] = template

    def get(self, name: str) -> QCRuleTemplate:
        if name not in self._templates:
            raise ValueError(f"QC rule template not found: {name}")
        return self._templates[name]

    def get_for_route_mode(
        self,
        route: RouteType,
        mode: ModeType = ModeType.NORMAL,
    ) -> QCRuleTemplate:
        for template in self._templates.values():
            if template.route == route and template.mode == mode:
                return template
        for template in self._templates.values():
            if template.route == RouteType.BOTH and template.mode == mode:
                return template
        for template in self._templates.values():
            if template.route == route and template.mode == ModeType.NORMAL:
                return template
        return self.get("default")

    def list_templates(self) -> list[str]:
        return list(self._templates.keys())

    def _register_defaults(self) -> None:
        self.register(
            QCRuleTemplate(
                name="co2_strict",
                route=RouteType.CO2,
                mode=ModeType.NORMAL,
                sample_count=SampleCountRule(min_count=10, max_missing=1),
                stability=StabilityRule(co2_max_std=1.0, h2o_max_std=0.3),
                outlier=OutlierRule(z_threshold=2.5, max_outlier_ratio=0.1),
                description="CO2 route strict QC rules",
            )
        )
        self.register(
            QCRuleTemplate(
                name="h2o_strict",
                route=RouteType.H2O,
                mode=ModeType.NORMAL,
                sample_count=SampleCountRule(min_count=10, max_missing=1),
                stability=StabilityRule(co2_max_std=2.0, h2o_max_std=0.2),
                outlier=OutlierRule(z_threshold=2.5, max_outlier_ratio=0.1),
                description="H2O route strict QC rules",
            )
        )
        self.register(
            QCRuleTemplate(
                name="fast_mode",
                route=RouteType.BOTH,
                mode=ModeType.FAST,
                sample_count=SampleCountRule(min_count=3, max_missing=2),
                stability=StabilityRule(co2_max_std=5.0, h2o_max_std=1.0),
                outlier=OutlierRule(z_threshold=3.5, max_outlier_ratio=0.3),
                description="Fast mode relaxed QC rules",
            )
        )
        self.register(
            QCRuleTemplate(
                name="verify_mode",
                route=RouteType.BOTH,
                mode=ModeType.VERIFY,
                sample_count=SampleCountRule(min_count=5, max_missing=1),
                stability=StabilityRule(co2_max_std=2.0, h2o_max_std=0.5),
                outlier=OutlierRule(z_threshold=3.0, max_outlier_ratio=0.2),
                description="Verify mode QC rules",
            )
        )
        self.register(
            QCRuleTemplate(
                name="subzero",
                route=RouteType.BOTH,
                mode=ModeType.SUBZERO,
                sample_count=SampleCountRule(min_count=8, max_missing=2),
                stability=StabilityRule(co2_max_std=3.0, h2o_max_std=1.0, temperature_max_std=1.0),
                outlier=OutlierRule(z_threshold=3.0, max_outlier_ratio=0.2),
                description="Subzero temperature QC rules",
            )
        )
        self.register(
            QCRuleTemplate(
                name="default",
                route=RouteType.BOTH,
                mode=ModeType.NORMAL,
                description="Default QC rules",
            )
        )
