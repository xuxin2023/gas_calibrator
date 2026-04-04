from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from .exporters import export_json
from .feature_builder import FeatureBuilder
from .marts import MART_BUILDERS


ANALYTICS_REPORT_SCHEMA_VERSION = "1.0"


class AnalyticsService:
    """Coordinates analytics feature building and mart execution."""

    def __init__(self, database):
        self.database = database
        self.feature_builder = FeatureBuilder(database)

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
        if report_name not in MART_BUILDERS:
            raise ValueError(f"Unsupported analytics report: {report_name}")
        data = MART_BUILDERS[report_name](features, run_id=run_id, analyzer_id=analyzer_id)
        return self._wrap_report(report_name, data, run_id=run_id, analyzer_id=analyzer_id)

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
            tuple(MART_BUILDERS.keys()),
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

    def _wrap_report(
        self,
        report_name: str,
        data: dict[str, Any],
        *,
        run_id: str | None,
        analyzer_id: str | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": ANALYTICS_REPORT_SCHEMA_VERSION,
            "report_name": report_name,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scope": {
                "run_id": run_id,
                "analyzer_id": analyzer_id,
            },
            "source": {
                "backend": self.database.settings.normalized_backend,
                "database_enabled": self.database.enabled,
            },
            "data": data,
        }
