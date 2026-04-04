from __future__ import annotations

from datetime import datetime
from typing import Any


MEASUREMENT_ANALYTICS_SCHEMA_VERSION = "1.0"
MEASUREMENT_FEATURE_SCHEMA_VERSION = "1.0"


def build_measurement_scope(
    *,
    run_id: str | None = None,
    analyzer_id: str | None = None,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "analyzer_id": analyzer_id,
    }


def build_measurement_feature_payload(
    *,
    run_id: str | None,
    analyzer_id: str | None,
    run_features: list[dict[str, Any]],
    frame_features: list[dict[str, Any]],
    analyzer_features: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": MEASUREMENT_FEATURE_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": build_measurement_scope(run_id=run_id, analyzer_id=analyzer_id),
        "run_features": run_features,
        "frame_features": frame_features,
        "analyzer_features": analyzer_features,
    }


def build_measurement_report_payload(
    *,
    report_name: str,
    data: dict[str, Any],
    database: Any,
    run_id: str | None,
    analyzer_id: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": MEASUREMENT_ANALYTICS_SCHEMA_VERSION,
        "report_name": report_name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": build_measurement_scope(run_id=run_id, analyzer_id=analyzer_id),
        "source": {
            "backend": database.settings.normalized_backend,
            "database_enabled": database.enabled,
        },
        "data": data,
    }
