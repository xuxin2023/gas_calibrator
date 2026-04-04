from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from .exporters import export_json
from .feature_builder import MeasurementFeatureBuilder
from .marts import MEASUREMENT_MART_BUILDERS
from .schemas import build_measurement_report_payload


class MeasurementAnalyticsService:
    """Coordinates measurement-frame feature building and mart execution."""

    def __init__(self, database):
        self.database = database
        self.feature_builder = MeasurementFeatureBuilder(database)

    def build_features(self, *, run_id: str | None = None, analyzer_id: str | None = None) -> dict[str, Any]:
        return self.feature_builder.build_features(run_id=run_id, analyzer_id=analyzer_id)

    def run_report(
        self,
        report_name: str,
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, Any]:
        features = self.build_features(run_id=run_id, analyzer_id=analyzer_id)
        return self.render_report(
            report_name,
            features=features,
            run_id=run_id,
            analyzer_id=analyzer_id,
        )

    def render_report(
        self,
        report_name: str,
        *,
        features: dict[str, Any],
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, Any]:
        if report_name not in MEASUREMENT_MART_BUILDERS:
            raise ValueError(f"Unsupported measurement analytics report: {report_name}")
        data = MEASUREMENT_MART_BUILDERS[report_name](features, run_id=run_id, analyzer_id=analyzer_id)
        return build_measurement_report_payload(
            report_name=report_name,
            data=data,
            database=self.database,
            run_id=run_id,
            analyzer_id=analyzer_id,
        )

    def run_reports(
        self,
        report_names: Sequence[str],
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        features = self.build_features(run_id=run_id, analyzer_id=analyzer_id)
        return {
            report_name: self.render_report(
                report_name,
                features=features,
                run_id=run_id,
                analyzer_id=analyzer_id,
            )
            for report_name in report_names
        }

    def run_all(
        self,
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        return self.run_reports(
            tuple(MEASUREMENT_MART_BUILDERS.keys()),
            run_id=run_id,
            analyzer_id=analyzer_id,
        )

    def export_report(
        self,
        report_name: str,
        path: str | Path,
        *,
        run_id: str | None = None,
        analyzer_id: str | None = None,
    ) -> Path:
        return export_json(path, self.run_report(report_name, run_id=run_id, analyzer_id=analyzer_id))
