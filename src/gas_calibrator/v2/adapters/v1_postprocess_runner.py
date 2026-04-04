"""Offline sidecar entry for V1 post-processing on top of V2 utilities.

This adapter is intentionally offline-only. It reads completed run artifacts,
generates metadata and reports, optionally imports them into database storage,
and never touches live device control unless `download=True` is explicitly
requested by the caller.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Optional, Sequence

from ..analytics.exporters import export_json
from ..analytics.measurement.exporters import export_json as export_measurement_json
from ..config import AIConfig, AppConfig, CoefficientsConfig
from ..core.models import CalibrationPoint, SamplingResult
from ..core.run_manifest import write_run_manifest
from ..exceptions import ConfigurationInvalidError, DataParseError
from ..export import export_ratio_poly_report_from_summary_files, load_summary_workbook_rows
from ..intelligence.runtime import AIRuntime
from ..intelligence.llm_client import is_mock_client
from ..qc import QCPipeline
from .offline_refit_runner import run_from_cli as run_offline_refit


ANALYTICS_REPORT_TARGETS = (
    ("run_kpis", "run_kpis", "run_kpis.json"),
    ("point_kpis", "point_kpis", "point_kpis.json"),
    ("drift_report", "drift_metrics", "drift_report.json"),
    ("analyzer_health", "analyzer_health", "analyzer_health.json"),
    ("fault_attribution", "fault_attribution", "fault_attribution.json"),
    ("coefficient_lineage", "traceability", "coefficient_lineage.json"),
)

MEASUREMENT_ANALYTICS_REPORT_TARGETS = (
    ("measurement_quality", "measurement_quality", "measurement_quality.json"),
    ("measurement_drift_report", "measurement_drift", "measurement_drift_report.json"),
    ("signal_anomalies", "signal_anomaly", "signal_anomalies.json"),
    ("context_attribution", "context_attribution", "context_attribution.json"),
    ("instrument_health", "instrument_health", "instrument_health.json"),
)


def _log(message: str) -> None:
    print(message)


def download_coefficients_to_analyzers(**kwargs: Any) -> dict[str, str]:
    """Lazy wrapper so default imports stay sidecar-safe."""
    from .analyzer_coefficient_downloader import download_coefficients_to_analyzers as impl

    return impl(**kwargs)


def _default_coefficients_payload() -> dict[str, Any]:
    return {
        "enabled": True,
        "auto_fit": True,
        "model": "ratio_poly_rt_p",
        "summary_columns": {
            "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
            "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
        },
    }


def _load_app_config(config_path: Optional[str]) -> AppConfig:
    if not config_path:
        return AppConfig.from_dict({"coefficients": _default_coefficients_payload()})
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationInvalidError("config_path", config_path, reason="config file does not exist")
    return AppConfig.from_json_file(str(path))


def _load_coefficients_config(config_path: Optional[str]) -> CoefficientsConfig:
    config = _load_app_config(config_path)
    coeff_cfg = config.coefficients
    coeff_cfg.enabled = True
    coeff_cfg.auto_fit = True
    return coeff_cfg


def _is_missing_sqlalchemy(exc: ModuleNotFoundError) -> bool:
    missing_name = str(getattr(exc, "name", "") or "")
    error_text = str(exc).lower()
    return (
        missing_name == "sqlalchemy"
        or missing_name.startswith("sqlalchemy.")
        or "sqlalchemy" in error_text
    )


def _sqlalchemy_skip_reason(scope: str) -> str:
    return (
        f"{scope} skipped in degraded mode because optional dependency 'sqlalchemy' is unavailable. "
        "Install sqlalchemy to enable database-backed import and analytics."
    )


def _load_storage_runtime() -> tuple[Any, Any, Any]:
    from ..storage.database import DatabaseManager, StorageSettings
    from ..storage.import_run import run_import

    return DatabaseManager, StorageSettings, run_import


def _load_analytics_service() -> Any:
    from ..analytics import AnalyticsService

    return AnalyticsService


def _load_measurement_analytics_service() -> Any:
    from ..analytics.measurement import MeasurementAnalyticsService

    return MeasurementAnalyticsService


try:
    AnalyticsService = _load_analytics_service()
except ModuleNotFoundError as exc:
    if _is_missing_sqlalchemy(exc):
        AnalyticsService = None
    else:
        raise

try:
    MeasurementAnalyticsService = _load_measurement_analytics_service()
except ModuleNotFoundError as exc:
    if _is_missing_sqlalchemy(exc):
        MeasurementAnalyticsService = None
    else:
        raise


def _iter_combined_summary_candidates(run_dir: Path) -> Iterable[Path]:
    for pattern in ("分析仪汇总*.xlsx", "分析仪汇总*.csv"):
        for path in sorted(run_dir.glob(pattern)):
            name = path.name
            if "分析仪汇总_水路_" in name or "分析仪汇总_气路_" in name:
                continue
            yield path


def _iter_split_summary_candidates(run_dir: Path, phase: str) -> Iterable[Path]:
    for pattern in (f"分析仪汇总_{phase}_*.xlsx", f"分析仪汇总_{phase}_*.csv"):
        for path in sorted(run_dir.glob(pattern)):
            yield path


def _resolve_run_dir(
    *,
    run_dir: Optional[str],
    latest_run_root: Optional[str],
    latest_run: bool,
) -> Optional[Path]:
    if run_dir:
        directory = Path(run_dir)
        if not directory.exists():
            raise ConfigurationInvalidError("run_dir", run_dir, reason="run directory does not exist")
        if not directory.is_dir():
            raise ConfigurationInvalidError("run_dir", run_dir, reason="run_dir must point to a directory")
        return directory.resolve()

    if not latest_run:
        return None

    if not latest_run_root:
        raise ConfigurationInvalidError("latest_run_root", latest_run_root, reason="latest_run requires latest_run_root")

    root = Path(latest_run_root)
    if not root.exists():
        raise ConfigurationInvalidError("latest_run_root", latest_run_root, reason="latest_run_root does not exist")
    candidates = [path for path in root.glob("run_*") if path.is_dir()]
    if not candidates:
        raise ConfigurationInvalidError("latest_run_root", latest_run_root, reason="no run_* directories were found")
    return max(candidates, key=lambda path: path.stat().st_mtime).resolve()


def _resolve_summary_paths(
    *,
    run_dir: Optional[Path],
    summary_paths: Optional[Sequence[str]],
) -> list[Path]:
    explicit = [Path(item).resolve() for item in (summary_paths or []) if str(item).strip()]
    if explicit:
        missing = [str(path) for path in explicit if not path.exists()]
        if missing:
            raise ConfigurationInvalidError("summary_paths", missing, reason="summary file does not exist")
        return explicit

    if run_dir is None:
        raise ConfigurationInvalidError("run_dir", run_dir, reason="run_dir or summary_paths is required")

    gas_candidates = list(_iter_split_summary_candidates(run_dir, "气路"))
    water_candidates = list(_iter_split_summary_candidates(run_dir, "水路"))
    if gas_candidates and water_candidates:
        gas_path = max(gas_candidates, key=lambda path: path.stat().st_mtime).resolve()
        water_path = max(water_candidates, key=lambda path: path.stat().st_mtime).resolve()
        return [gas_path, water_path]

    candidates = list(_iter_combined_summary_candidates(run_dir))
    if not candidates:
        raise ConfigurationInvalidError(
            "run_dir",
            str(run_dir),
            reason="no split water/gas summary workbooks or combined summary workbook were found",
        )
    return [max(candidates, key=lambda path: path.stat().st_mtime).resolve()]


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", "null", "None"):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _load_summary_runtime_context(run_dir: Optional[Path]) -> dict[str, Any]:
    if run_dir is None:
        return {}
    return _load_optional_json(run_dir / "summary.json")


def _build_static_session(
    *,
    run_id: str,
    config: AppConfig,
    run_dir: Path,
    enabled_devices: Sequence[str],
    started_at: datetime | None,
    ended_at: datetime | None,
) -> Any:
    return SimpleNamespace(
        run_id=run_id,
        config=config,
        started_at=started_at,
        ended_at=ended_at,
        enabled_devices=set(enabled_devices),
        output_dir=run_dir,
    )


def _resolve_source_points_file(run_dir: Optional[Path], summary_paths: Sequence[Path]) -> str | None:
    if run_dir is not None:
        for name in ("points.csv", "points.xlsx", "points.json"):
            candidate = run_dir / name
            if candidate.exists():
                return str(candidate)
    if summary_paths:
        return str(summary_paths[0])
    return None


def _manifest_step(
    *,
    run_dir: Optional[Path],
    run_id: str,
    config: AppConfig,
    summary_context: dict[str, Any],
    summary_paths: Sequence[Path],
) -> dict[str, Any]:
    if run_dir is None:
        return {"status": "skipped", "reason": "run directory unavailable"}

    stats = summary_context.get("stats") or {}
    status = summary_context.get("status") or {}
    ended_at = _parse_datetime(summary_context.get("ended_at") or summary_context.get("generated_at"))
    elapsed_s = _coerce_float(status.get("elapsed_s")) or 0.0
    started_at = None
    if ended_at is not None:
        try:
            started_at = ended_at.timestamp() - elapsed_s
            started_at = datetime.fromtimestamp(started_at, tz=ended_at.tzinfo)
        except Exception:
            started_at = None

    session = _build_static_session(
        run_id=run_id,
        config=config,
        run_dir=run_dir,
        enabled_devices=list(stats.get("enabled_devices", [])),
        started_at=started_at,
        ended_at=ended_at,
    )
    manifest_path = write_run_manifest(
        run_dir,
        session,
        source_points_file=_resolve_source_points_file(run_dir, summary_paths),
        operator=summary_context.get("operator") or stats.get("operator"),
    )
    return {"status": "completed", "path": str(manifest_path)}


def _build_storage_settings(config_path: Optional[str], dsn: Optional[str]) -> Any:
    _, StorageSettings, _ = _load_storage_runtime()
    config = _load_app_config(config_path)
    settings = StorageSettings.from_config(config.storage)
    if dsn:
        settings.dsn = str(dsn)
        lowered = str(dsn).strip().lower()
        if lowered.startswith("sqlite"):
            settings.backend = "sqlite"
        elif lowered.startswith("postgres") or lowered.startswith("postgresql"):
            settings.backend = "postgresql"
    return settings


def _database_import_step(
    *,
    run_dir: Optional[Path],
    artifact_dir: Optional[Path],
    config_path: Optional[str],
    dsn: Optional[str],
    stage: str,
) -> dict[str, Any]:
    if run_dir is None:
        return {"status": "skipped", "reason": "run directory unavailable"}
    try:
        _, _, run_import = _load_storage_runtime()
        settings = _build_storage_settings(config_path, dsn)
    except ModuleNotFoundError as exc:
        if _is_missing_sqlalchemy(exc):
            return {
                "status": "skipped",
                "stage": stage,
                "dependency": "sqlalchemy",
                "reason": _sqlalchemy_skip_reason("database import"),
            }
        raise
    result = run_import(
        run_dir=run_dir,
        settings=settings,
        stage=stage,
        init_schema=True,
        artifact_dir=artifact_dir,
    )
    return {"status": "completed", **result}


def _local_analytics_settings(
    target_dir: Path,
    *,
    directory_name: str = "analytics",
    database_name: str = "analytics.sqlite",
) -> Any:
    _, StorageSettings, _ = _load_storage_runtime()
    analytics_dir = target_dir / directory_name
    analytics_dir.mkdir(parents=True, exist_ok=True)
    return StorageSettings(
        backend="sqlite",
        database=str(analytics_dir / database_name),
    )


def _analytics_step(
    *,
    run_dir: Optional[Path],
    target_dir: Path,
    run_id: str,
    config_path: Optional[str],
    dsn: Optional[str],
    import_db: bool,
    raw_database_step: dict[str, Any],
    enrich_database_step: dict[str, Any],
    run_analytics: bool,
    skip_analytics: bool,
) -> dict[str, Any]:
    if skip_analytics or not run_analytics:
        return {"status": "skipped", "reason": "analytics disabled"}
    if run_dir is None:
        return {"status": "skipped", "reason": "run directory unavailable"}

    analytics_dir = target_dir / "analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    analytics_reports: dict[str, dict[str, Any]] = {}
    failures: dict[str, dict[str, Any]] = {}
    bootstrap: dict[str, Any] | None = None

    try:
        DatabaseManager, _, run_import = _load_storage_runtime()
        AnalyticsService = _load_analytics_service()
    except ModuleNotFoundError as exc:
        if _is_missing_sqlalchemy(exc):
            return {
                "status": "skipped",
                "dir": str(analytics_dir),
                "source_mode": "dependency_unavailable",
                "bootstrap": None,
                "raw_database_status": raw_database_step.get("status"),
                "enrich_database_status": enrich_database_step.get("status"),
                "reports": analytics_reports,
                "failures": failures,
                "dependency": "sqlalchemy",
                "reason": _sqlalchemy_skip_reason("analytics"),
            }
        raise

    settings: Any = None
    source_mode = "configured_database"
    if import_db and raw_database_step.get("status") != "failed":
        settings = _build_storage_settings(config_path, dsn)
        health = DatabaseManager(settings).health_check()
        if not health.get("ok"):
            settings = None
            source_mode = "local_sqlite_fallback"

    if settings is None:
        settings = _local_analytics_settings(target_dir)
        bootstrap = run_import(
            run_dir=run_dir,
            settings=settings,
            stage="all",
            init_schema=True,
            artifact_dir=target_dir,
        )
        source_mode = "local_sqlite"

    database = DatabaseManager(settings)
    try:
        service = AnalyticsService(database)
        features = service.build_features(run_id=run_id)
        for artifact_name, report_name, filename in ANALYTICS_REPORT_TARGETS:
            target_path = analytics_dir / filename
            try:
                report = service.render_report(
                    report_name,
                    features=features,
                    run_id=run_id,
                )
                export_json(target_path, report)
            except Exception as exc:
                failures[artifact_name] = {
                    "status": "failed",
                    "report_name": report_name,
                    "path": str(target_path),
                    "error": str(exc),
                }
                continue
            analytics_reports[artifact_name] = {
                "status": "completed",
                "report_name": report_name,
                "path": str(target_path),
            }
    except Exception as exc:
        return {
            "status": "failed",
            "dir": str(analytics_dir),
            "source_mode": source_mode,
            "bootstrap": bootstrap,
            "reports": analytics_reports,
            "failures": {"analytics": {"status": "failed", "error": str(exc)}},
        }
    finally:
        database.dispose()

    if analytics_reports and not failures:
        status = "completed"
    elif analytics_reports and failures:
        status = "partial"
    elif failures:
        status = "failed"
    else:
        status = "skipped"

    return {
        "status": status,
        "dir": str(analytics_dir),
        "source_mode": source_mode,
        "bootstrap": bootstrap,
        "raw_database_status": raw_database_step.get("status"),
        "enrich_database_status": enrich_database_step.get("status"),
        "reports": analytics_reports,
        "failures": failures,
    }


def _measurement_analytics_step(
    *,
    run_dir: Optional[Path],
    target_dir: Path,
    run_id: str,
    config_path: Optional[str],
    dsn: Optional[str],
    import_db: bool,
    raw_database_step: dict[str, Any],
    enrich_database_step: dict[str, Any],
    run_measurement_analytics: bool,
    skip_measurement_analytics: bool,
) -> dict[str, Any]:
    if skip_measurement_analytics or not run_measurement_analytics:
        return {"status": "skipped", "reason": "measurement analytics disabled"}
    if run_dir is None:
        return {"status": "skipped", "reason": "run directory unavailable"}

    analytics_dir = target_dir / "measurement_analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    analytics_reports: dict[str, dict[str, Any]] = {}
    failures: dict[str, dict[str, Any]] = {}
    bootstrap: dict[str, Any] | None = None

    try:
        DatabaseManager, _, run_import = _load_storage_runtime()
        MeasurementAnalyticsService = _load_measurement_analytics_service()
    except ModuleNotFoundError as exc:
        if _is_missing_sqlalchemy(exc):
            return {
                "status": "skipped",
                "dir": str(analytics_dir),
                "source_mode": "dependency_unavailable",
                "bootstrap": None,
                "raw_database_status": raw_database_step.get("status"),
                "enrich_database_status": enrich_database_step.get("status"),
                "reports": analytics_reports,
                "failures": failures,
                "dependency": "sqlalchemy",
                "reason": _sqlalchemy_skip_reason("measurement analytics"),
            }
        raise

    settings: Any = None
    source_mode = "configured_database"
    if import_db and raw_database_step.get("status") != "failed":
        settings = _build_storage_settings(config_path, dsn)
        health = DatabaseManager(settings).health_check()
        if not health.get("ok"):
            settings = None
            source_mode = "local_sqlite_fallback"

    if settings is None:
        settings = _local_analytics_settings(
            target_dir,
            directory_name="measurement_analytics",
            database_name="measurement_analytics.sqlite",
        )
        bootstrap = run_import(
            run_dir=run_dir,
            settings=settings,
            stage="all",
            init_schema=True,
            artifact_dir=target_dir,
        )
        source_mode = "local_sqlite"

    database = DatabaseManager(settings)
    try:
        service = MeasurementAnalyticsService(database)
        features = service.build_features(run_id=run_id)
        for artifact_name, report_name, filename in MEASUREMENT_ANALYTICS_REPORT_TARGETS:
            target_path = analytics_dir / filename
            try:
                report = service.render_report(
                    report_name,
                    features=features,
                    run_id=run_id,
                )
                export_measurement_json(target_path, report)
            except Exception as exc:
                failures[artifact_name] = {
                    "status": "failed",
                    "report_name": report_name,
                    "path": str(target_path),
                    "error": str(exc),
                }
                continue
            analytics_reports[artifact_name] = {
                "status": "completed",
                "report_name": report_name,
                "path": str(target_path),
            }
    except Exception as exc:
        return {
            "status": "failed",
            "dir": str(analytics_dir),
            "source_mode": source_mode,
            "bootstrap": bootstrap,
            "reports": analytics_reports,
            "failures": {"measurement_analytics": {"status": "failed", "error": str(exc)}},
        }
    finally:
        database.dispose()

    if analytics_reports and not failures:
        status = "completed"
    elif analytics_reports and failures:
        status = "partial"
    elif failures:
        status = "failed"
    else:
        status = "skipped"

    return {
        "status": status,
        "dir": str(analytics_dir),
        "source_mode": source_mode,
        "bootstrap": bootstrap,
        "raw_database_status": raw_database_step.get("status"),
        "enrich_database_status": enrich_database_step.get("status"),
        "reports": analytics_reports,
        "failures": failures,
    }


def _build_point(payload: dict[str, Any]) -> CalibrationPoint:
    point_index = _coerce_int(payload.get("index"))
    if point_index is None:
        raise DataParseError("results.json", reason="sample point.index is missing")
    temperature_c = _coerce_float(payload.get("temperature_c"))
    if temperature_c is None:
        raise DataParseError("results.json", reason=f"point {point_index} temperature_c is missing")
    return CalibrationPoint(
        index=point_index,
        temperature_c=temperature_c,
        co2_ppm=_coerce_float(payload.get("co2_ppm")),
        humidity_pct=_coerce_float(payload.get("humidity_pct")),
        pressure_hpa=_coerce_float(payload.get("pressure_hpa")),
        route=str(payload.get("route") or "co2"),
        humidity_generator_temp_c=_coerce_float(payload.get("humidity_generator_temp_c")),
        dewpoint_c=_coerce_float(payload.get("dewpoint_c")),
        h2o_mmol=_coerce_float(payload.get("h2o_mmol")),
        raw_h2o=payload.get("raw_h2o"),
        co2_group=payload.get("co2_group"),
    )


def _build_sampling_result(payload: dict[str, Any]) -> SamplingResult:
    point = _build_point(dict(payload.get("point") or {}))
    timestamp = _parse_datetime(payload.get("timestamp"))
    if timestamp is None:
        raise DataParseError("results.json", reason=f"sample timestamp is missing for point {point.index}")
    return SamplingResult(
        point=point,
        analyzer_id=str(payload.get("analyzer_id") or "unknown_analyzer"),
        timestamp=timestamp,
        co2_ppm=_coerce_float(payload.get("co2_ppm")),
        h2o_mmol=_coerce_float(payload.get("h2o_mmol")),
        h2o_signal=_coerce_float(payload.get("h2o_signal")),
        co2_signal=_coerce_float(payload.get("co2_signal")),
        co2_ratio_f=_coerce_float(payload.get("co2_ratio_f")),
        co2_ratio_raw=_coerce_float(payload.get("co2_ratio_raw")),
        h2o_ratio_f=_coerce_float(payload.get("h2o_ratio_f")),
        h2o_ratio_raw=_coerce_float(payload.get("h2o_ratio_raw")),
        ref_signal=_coerce_float(payload.get("ref_signal")),
        temperature_c=_coerce_float(payload.get("temperature_c")),
        pressure_hpa=_coerce_float(payload.get("pressure_hpa")),
        dew_point_c=_coerce_float(payload.get("dew_point_c")),
        analyzer_pressure_kpa=_coerce_float(payload.get("analyzer_pressure_kpa")),
        analyzer_chamber_temp_c=_coerce_float(payload.get("analyzer_chamber_temp_c")),
        case_temp_c=_coerce_float(payload.get("case_temp_c")),
        point_phase=str(payload.get("point_phase") or ""),
        point_tag=str(payload.get("point_tag") or ""),
        stability_time_s=_coerce_float(payload.get("stability_time_s")),
        total_time_s=_coerce_float(payload.get("total_time_s")),
    )


def _qc_step(
    *,
    run_dir: Optional[Path],
    target_dir: Path,
    run_id: str,
    config: AppConfig,
    skip_qc: bool,
    skip_ai: bool,
) -> dict[str, Any]:
    if skip_qc:
        return {"status": "skipped", "reason": "skip_qc enabled"}
    if run_dir is None:
        return {"status": "skipped", "reason": "run directory unavailable"}

    results_path = run_dir / "results.json"
    if not results_path.exists():
        return {"status": "skipped", "reason": "results.json not found"}

    payload = _load_optional_json(results_path)
    raw_samples = payload.get("samples")
    if not isinstance(raw_samples, list) or not raw_samples:
        return {"status": "skipped", "reason": "results.json does not contain samples"}

    grouped_samples: dict[int, list[SamplingResult]] = {}
    point_map: dict[int, CalibrationPoint] = {}
    for item in raw_samples:
        if not isinstance(item, dict):
            continue
        sample = _build_sampling_result(item)
        grouped_samples.setdefault(sample.point.index, []).append(sample)
        point_map[sample.point.index] = sample.point

    if not point_map:
        return {"status": "skipped", "reason": "no valid samples were parsed from results.json"}

    ai_runtime = None
    ai_config = AIConfig(enabled=False)
    if not skip_ai:
        ai_config = config.ai
        if ai_config.enabled:
            ai_runtime = AIRuntime.from_config(ai_config)

    pipeline = QCPipeline(
        config.qc,
        run_id=run_id,
        qc_explainer=None if ai_runtime is None else ai_runtime.qc_explainer,
        ai_config=ai_config,
    )
    ordered_points = [point_map[index] for index in sorted(point_map)]
    validations, run_score, report = pipeline.process_run(
        [(point, list(grouped_samples.get(point.index, []))) for point in ordered_points]
    )

    reporter = pipeline.reporter
    qc_json = target_dir / "qc_report.json"
    qc_csv = target_dir / "qc_report.csv"
    reporter.export_json(report, qc_json)
    _write_qc_csv(report.point_details, qc_csv)
    return {
        "status": "completed",
        "json": str(qc_json),
        "csv": str(qc_csv),
        "total_points": report.total_points,
        "invalid_points": report.invalid_points,
        "overall_score": run_score.overall_score,
    }


def _report_step(
    *,
    summary_paths: Sequence[Path],
    target_dir: Path,
    coeff_cfg: CoefficientsConfig,
) -> dict[str, Any]:
    output_path = export_ratio_poly_report_from_summary_files(
        [str(path) for path in summary_paths],
        out_dir=target_dir,
        coeff_cfg=coeff_cfg,
    )
    if output_path is None:
        raise DataParseError("v1_postprocess_runner", reason="failed to generate ratio-poly report")
    return {"status": "completed", "path": str(output_path)}


def _refit_step(
    *,
    summary_paths: Sequence[Path],
    config_path: Optional[str],
    target_dir: Path,
    skip_refit: bool,
) -> dict[str, Any]:
    if skip_refit:
        return {"status": "skipped", "reason": "skip_refit enabled"}
    if len(summary_paths) != 1:
        return {"status": "skipped", "reason": "refit requires exactly one combined summary workbook"}

    summary_frame = load_summary_workbook_rows(summary_paths)
    if summary_frame.empty:
        return {"status": "skipped", "reason": "combined summary workbook is empty"}

    analyzers = sorted(str(value).strip().upper() for value in summary_frame["Analyzer"].dropna().unique())
    if not analyzers:
        return {"status": "skipped", "reason": "no analyzers found in combined summary workbook"}

    exported_runs: list[dict[str, Any]] = []
    for analyzer in analyzers:
        for gas in ("co2", "h2o"):
            try:
                exported = run_offline_refit(
                    input_path=str(summary_paths[0]),
                    gas_type=gas,
                    analyzer_id=analyzer,
                    config_path=config_path,
                    sheet_name=analyzer,
                    output_dir=str(target_dir / "offline_refit" / analyzer / gas),
                )
            except Exception as exc:
                exported_runs.append(
                    {
                        "analyzer": analyzer,
                        "gas": gas,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
            else:
                exported_runs.append(
                    {
                        "analyzer": analyzer,
                        "gas": gas,
                        "status": "completed",
                        **{key: str(value) for key, value in exported.items()},
                    }
                )
    return {"status": "completed", "runs": exported_runs}


def _download_step(
    *,
    download: bool,
    config_path: Optional[str],
    report_path: str,
    target_dir: Path,
) -> dict[str, Any]:
    if not download:
        return {"status": "skipped", "reason": "download disabled"}
    if not config_path:
        raise ConfigurationInvalidError("config_path", config_path, reason="download requires config_path")
    outputs = download_coefficients_to_analyzers(
        report_path=Path(report_path),
        config_path=config_path,
        output_dir=target_dir,
    )
    return {"status": "completed", **{key: str(value) for key, value in outputs.items()}}


def _write_qc_csv(point_details: Sequence[dict[str, Any]], path: Path) -> None:
    fieldnames = [
        "point_index",
        "route",
        "temperature_c",
        "co2_ppm",
        "quality_score",
        "valid",
        "recommendation",
        "reason",
        "ai_explanation",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in point_details:
            writer.writerow(dict(row))


def _write_postprocess_summary(target_dir: Path, payload: dict[str, Any]) -> Path:
    path = target_dir / "calibration_coefficients_postprocess_summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_markdown(path: Path, title: str, body: str) -> Path:
    content = f"# {title}\n\n{str(body or '').strip()}\n"
    path.write_text(content, encoding="utf-8")
    return path


def _build_ai_skip_note(run_id: str, reason: str) -> str:
    return (
        f"本次运行 `{run_id}` 的 AI 离线解释已跳过。\n\n"
        f"- 原因：{reason}\n"
        "- 后处理链其余步骤仍可正常完成。\n"
        "- AI 仅用于解释、总结和建议，不会修改系数，也不会触发任何设备动作。"
    )


def _build_ai_fallback_note(run_id: str, reason: str) -> str:
    return (
        f"本次运行 `{run_id}` 的 AI 离线解释未能完成，已退化为本地说明。\n\n"
        f"- 原因：{reason}\n"
        "- 后处理链已继续完成；QC、report、refit 等步骤不受影响。\n"
        "- AI 输出仅用于解释、总结和建议，不会修改系数，也不会触发任何设备动作。"
    )


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _failed_qc_points(qc_payload: dict[str, Any]) -> list[dict[str, Any]]:
    details = qc_payload.get("point_details")
    if not isinstance(details, list):
        return []
    failed: list[dict[str, Any]] = []
    for item in details:
        if not isinstance(item, dict):
            continue
        if bool(item.get("valid")):
            continue
        failed.append(dict(item))
    return failed


def _ai_step(
    *,
    run_dir: Optional[Path],
    target_dir: Path,
    run_id: str,
    config: AppConfig,
    qc_step: dict[str, Any],
    skip_ai: bool,
) -> dict[str, Any]:
    summary_path = target_dir / "ai_run_summary.md"
    anomaly_path = target_dir / "ai_anomaly_note.md"

    if skip_ai:
        _write_markdown(summary_path, "AI Run Summary", _build_ai_skip_note(run_id, "skip_ai enabled"))
        return {
            "status": "skipped",
            "provider": str(config.ai.provider or "mock"),
            "client_mode": "skipped",
            "run_summary": {"status": "skipped", "path": str(summary_path)},
            "anomaly_note": {"status": "skipped", "reason": "skip_ai enabled"},
        }

    if run_dir is None:
        _write_markdown(summary_path, "AI Run Summary", _build_ai_fallback_note(run_id, "run directory unavailable"))
        return {
            "status": "fallback",
            "provider": str(config.ai.provider or "mock"),
            "client_mode": "unavailable",
            "run_summary": {"status": "fallback", "path": str(summary_path)},
            "anomaly_note": {"status": "skipped", "reason": "run directory unavailable"},
        }

    provider = str(config.ai.provider or "mock")
    try:
        ai_runtime = AIRuntime.from_config(config.ai)
        client_mode = "mock" if is_mock_client(ai_runtime.llm) else provider
    except Exception as exc:
        _write_markdown(summary_path, "AI Run Summary", _build_ai_fallback_note(run_id, str(exc)))
        return {
            "status": "fallback",
            "provider": provider,
            "client_mode": "fallback_error",
            "run_summary": {"status": "fallback", "path": str(summary_path)},
            "anomaly_note": {"status": "skipped", "reason": "AI runtime unavailable"},
            "error": str(exc),
        }

    qc_payload = _load_optional_json(Path(qc_step["json"])) if qc_step.get("json") else _load_optional_json(run_dir / "qc_report.json")
    failed_points = _failed_qc_points(qc_payload)
    io_events = _load_csv_rows(run_dir / "io_log.csv")

    anomaly_step: dict[str, Any]
    anomaly_text = ""
    if failed_points:
        try:
            anomaly_text = ai_runtime.anomaly_advisor.diagnose_run(
                failed_points=failed_points,
                device_events=io_events,
                alarms=[],
            )
            _write_markdown(anomaly_path, "AI Anomaly Note", anomaly_text)
            anomaly_step = {"status": "completed", "path": str(anomaly_path)}
        except Exception as exc:
            anomaly_text = _build_ai_fallback_note(run_id, f"anomaly diagnosis failed: {exc}")
            _write_markdown(anomaly_path, "AI Anomaly Note", anomaly_text)
            anomaly_step = {"status": "fallback", "path": str(anomaly_path), "error": str(exc)}
    else:
        anomaly_step = {"status": "skipped", "reason": "no failed QC points"}

    try:
        summary_text = ai_runtime.summarizer.summarize_run_directory(
            run_dir,
            anomaly_diagnosis=anomaly_text,
        )
        _write_markdown(summary_path, "AI Run Summary", summary_text)
        return {
            "status": "completed",
            "provider": provider,
            "client_mode": client_mode,
            "run_summary": {"status": "completed", "path": str(summary_path)},
            "anomaly_note": anomaly_step,
        }
    except Exception as exc:
        _write_markdown(summary_path, "AI Run Summary", _build_ai_fallback_note(run_id, str(exc)))
        return {
            "status": "fallback",
            "provider": provider,
            "client_mode": client_mode,
            "run_summary": {"status": "fallback", "path": str(summary_path)},
            "anomaly_note": anomaly_step,
            "error": str(exc),
        }


def run_from_cli(
    *,
    run_dir: Optional[str] = None,
    summary_paths: Optional[Sequence[str]] = None,
    config_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    download: bool = False,
    import_db: bool = False,
    dsn: Optional[str] = None,
    skip_qc: bool = False,
    skip_refit: bool = False,
    skip_ai: bool = False,
    latest_run_root: Optional[str] = None,
    latest_run: bool = False,
    run_analytics: bool = True,
    skip_analytics: bool = False,
    analytics_only: bool = False,
    run_measurement_analytics: bool = True,
    skip_measurement_analytics: bool = False,
    measurement_analytics_only: bool = False,
) -> Dict[str, Any]:
    resolved_run_dir = _resolve_run_dir(run_dir=run_dir, latest_run_root=latest_run_root, latest_run=latest_run)
    resolved_summary_paths = _resolve_summary_paths(run_dir=resolved_run_dir, summary_paths=summary_paths)
    coeff_cfg = _load_coefficients_config(config_path)
    if str(coeff_cfg.model).strip().lower() != "ratio_poly_rt_p":
        raise ConfigurationInvalidError("coefficients.model", coeff_cfg.model, reason="only ratio_poly_rt_p is supported")

    app_config = _load_app_config(config_path)
    target_dir = Path(output_dir).resolve() if output_dir else (
        resolved_run_dir if resolved_run_dir is not None else resolved_summary_paths[0].resolve().parent
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    summary_context = _load_summary_runtime_context(resolved_run_dir)
    run_id = str(summary_context.get("run_id") or (resolved_run_dir.name if resolved_run_dir else resolved_summary_paths[0].stem))
    effective_skip_qc = bool(skip_qc or analytics_only or measurement_analytics_only)
    effective_skip_refit = bool(skip_refit or analytics_only or measurement_analytics_only)
    effective_skip_ai = bool(skip_ai or analytics_only or measurement_analytics_only)
    effective_run_analytics = False if measurement_analytics_only else bool(run_analytics or analytics_only)
    effective_skip_analytics = True if measurement_analytics_only else bool(skip_analytics and not analytics_only)
    effective_run_measurement_analytics = bool(run_measurement_analytics or measurement_analytics_only)
    effective_skip_measurement_analytics = bool(skip_measurement_analytics and not measurement_analytics_only)

    _log("V1 offline postprocess sidecar started")
    _log(f"Summary inputs: {', '.join(str(path) for path in resolved_summary_paths)}")
    if resolved_run_dir is not None:
        _log(f"Run directory: {resolved_run_dir}")

    manifest_step = _manifest_step(
        run_dir=resolved_run_dir,
        run_id=run_id,
        config=app_config,
        summary_context=summary_context,
        summary_paths=resolved_summary_paths,
    )
    if manifest_step["status"] == "completed":
        _log(f"Manifest: {manifest_step['path']}")

    raw_database_step: dict[str, Any]
    if import_db:
        try:
            raw_database_step = _database_import_step(
                run_dir=resolved_run_dir,
                artifact_dir=resolved_run_dir,
                config_path=config_path,
                dsn=dsn,
                stage="raw",
            )
            if raw_database_step["status"] == "completed":
                _log("Database raw import completed")
            elif raw_database_step["status"] == "skipped":
                _log(f"Database raw import skipped: {raw_database_step.get('reason', 'database import unavailable')}")
            else:
                _log("Database raw import finished with degraded status")
        except Exception as exc:
            raw_database_step = {"status": "failed", "stage": "raw", "error": str(exc)}
            _log(f"Database raw import failed: {exc}")
    else:
        raw_database_step = {"status": "skipped", "stage": "raw", "reason": "import_db disabled"}

    qc_step: dict[str, Any]
    try:
        qc_step = _qc_step(
            run_dir=resolved_run_dir,
            target_dir=target_dir,
            run_id=run_id,
            config=app_config,
            skip_qc=effective_skip_qc,
            skip_ai=effective_skip_ai,
        )
        if qc_step["status"] == "completed":
            _log(f"QC report: {qc_step['json']}")
        elif qc_step["status"] == "skipped":
            _log(f"QC skipped: {qc_step['reason']}")
    except Exception as exc:
        qc_step = {"status": "failed", "error": str(exc)}
        _log(f"QC failed: {exc}")

    if analytics_only or measurement_analytics_only:
        skip_reason = "measurement_analytics_only enabled" if measurement_analytics_only else "analytics_only enabled"
        report_step = {"status": "skipped", "reason": skip_reason}
        _log(f"Report skipped: {skip_reason}")
    else:
        report_step = _report_step(summary_paths=resolved_summary_paths, target_dir=target_dir, coeff_cfg=coeff_cfg)
        _log(f"Report: {report_step['path']}")

    try:
        refit_step = _refit_step(
            summary_paths=resolved_summary_paths,
            config_path=config_path,
            target_dir=target_dir,
            skip_refit=effective_skip_refit,
        )
        if refit_step["status"] == "completed":
            _log("Offline refit finished")
        elif refit_step["status"] == "skipped":
            _log(f"Offline refit skipped: {refit_step['reason']}")
    except Exception as exc:
        refit_step = {"status": "failed", "error": str(exc)}
        _log(f"Offline refit failed: {exc}")

    ai_step = _ai_step(
        run_dir=resolved_run_dir,
        target_dir=target_dir,
        run_id=run_id,
        config=app_config,
        qc_step=qc_step,
        skip_ai=effective_skip_ai,
    )
    if ai_step["status"] == "completed":
        _log(f"AI run summary: {ai_step['run_summary']['path']}")
    elif ai_step["status"] == "skipped":
        _log("AI postprocess skipped")
    else:
        _log("AI postprocess finished with fallback")

    provisional_database_step = {
        "status": "pending" if import_db else "skipped",
        "raw": raw_database_step,
        "enrich": {"status": "pending", "stage": "enrich"} if import_db else {"status": "skipped", "stage": "enrich", "reason": "import_db disabled"},
    }
    _write_postprocess_summary(
        target_dir,
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id,
            "run_dir": None if resolved_run_dir is None else str(resolved_run_dir),
            "target_dir": str(target_dir),
            "summary_paths": [str(path) for path in resolved_summary_paths],
            "flags": {
                "import_db": bool(import_db),
                "skip_qc": bool(effective_skip_qc),
                "skip_refit": bool(effective_skip_refit),
                "skip_ai": bool(effective_skip_ai),
                "run_analytics": bool(effective_run_analytics),
                "skip_analytics": bool(effective_skip_analytics),
                "analytics_only": bool(analytics_only),
                "run_measurement_analytics": bool(effective_run_measurement_analytics),
                "skip_measurement_analytics": bool(effective_skip_measurement_analytics),
                "measurement_analytics_only": bool(measurement_analytics_only),
                "download": bool(download),
                "latest_run": bool(latest_run),
            },
            "manifest": manifest_step,
            "database_import": provisional_database_step,
            "qc": qc_step,
            "report": report_step,
            "refit": refit_step,
            "ai": ai_step,
            "analytics": {
                "status": "pending" if effective_run_analytics and not effective_skip_analytics else "skipped",
                "reason": None if effective_run_analytics and not effective_skip_analytics else "analytics disabled",
            },
            "measurement_analytics": {
                "status": (
                    "pending"
                    if effective_run_measurement_analytics and not effective_skip_measurement_analytics
                    else "skipped"
                ),
                "reason": (
                    None
                    if effective_run_measurement_analytics and not effective_skip_measurement_analytics
                    else "measurement analytics disabled"
                ),
            },
            "download": {"status": "pending" if download else "skipped", "reason": None if download else "download disabled"},
        },
    )

    enrich_database_step: dict[str, Any]
    if import_db:
        if raw_database_step["status"] == "failed":
            enrich_database_step = {"status": "skipped", "stage": "enrich", "reason": "raw import failed"}
            _log("Database enrich import skipped because raw import failed")
        else:
            try:
                enrich_database_step = _database_import_step(
                    run_dir=resolved_run_dir,
                    artifact_dir=target_dir,
                    config_path=config_path,
                    dsn=dsn,
                    stage="enrich",
                )
                if enrich_database_step["status"] == "completed":
                    _log("Database enrich import completed")
                elif enrich_database_step["status"] == "skipped":
                    _log(f"Database enrich import skipped: {enrich_database_step.get('reason', 'database import unavailable')}")
                else:
                    _log("Database enrich import finished with degraded status")
            except Exception as exc:
                enrich_database_step = {"status": "failed", "stage": "enrich", "error": str(exc)}
                _log(f"Database enrich import failed: {exc}")
    else:
        enrich_database_step = {"status": "skipped", "stage": "enrich", "reason": "import_db disabled"}

    analytics_step = _analytics_step(
        run_dir=resolved_run_dir,
        target_dir=target_dir,
        run_id=run_id,
        config_path=config_path,
        dsn=dsn,
        import_db=import_db,
        raw_database_step=raw_database_step,
        enrich_database_step=enrich_database_step,
        run_analytics=effective_run_analytics,
        skip_analytics=effective_skip_analytics,
    )
    if analytics_step["status"] == "completed":
        _log(f"Analytics completed: {analytics_step['dir']}")
    elif analytics_step["status"] == "partial":
        _log("Analytics completed with partial failures")
    elif analytics_step["status"] == "skipped":
        _log("Analytics skipped")
    else:
        _log("Analytics failed")

    measurement_analytics_step = _measurement_analytics_step(
        run_dir=resolved_run_dir,
        target_dir=target_dir,
        run_id=run_id,
        config_path=config_path,
        dsn=dsn,
        import_db=import_db,
        raw_database_step=raw_database_step,
        enrich_database_step=enrich_database_step,
        run_measurement_analytics=effective_run_measurement_analytics,
        skip_measurement_analytics=effective_skip_measurement_analytics,
    )
    if measurement_analytics_step["status"] == "completed":
        _log(f"Measurement analytics completed: {measurement_analytics_step['dir']}")
    elif measurement_analytics_step["status"] == "partial":
        _log("Measurement analytics completed with partial failures")
    elif measurement_analytics_step["status"] == "skipped":
        _log("Measurement analytics skipped")
    else:
        _log("Measurement analytics failed")

    download_step = _download_step(
        download=bool(download and not analytics_only and not measurement_analytics_only),
        config_path=config_path,
        report_path=report_step.get("path", ""),
        target_dir=target_dir,
    )
    if download_step["status"] == "completed":
        _log("Coefficient download completed")
    elif (analytics_only or measurement_analytics_only) and download:
        download_step = {
            "status": "skipped",
            "reason": "measurement_analytics_only enabled" if measurement_analytics_only else "analytics_only enabled",
        }

    database_statuses = {raw_database_step["status"], enrich_database_step["status"]}
    if database_statuses == {"completed"}:
        database_status = "completed"
    elif "failed" in database_statuses and "completed" in database_statuses:
        database_status = "partial"
    elif "failed" in database_statuses:
        database_status = "failed"
    elif database_statuses == {"skipped"}:
        database_status = "skipped"
    else:
        database_status = "completed"

    summary_payload: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": None if resolved_run_dir is None else str(resolved_run_dir),
        "target_dir": str(target_dir),
        "summary_paths": [str(path) for path in resolved_summary_paths],
        "flags": {
            "import_db": bool(import_db),
            "skip_qc": bool(effective_skip_qc),
            "skip_refit": bool(effective_skip_refit),
            "skip_ai": bool(effective_skip_ai),
            "run_analytics": bool(effective_run_analytics),
            "skip_analytics": bool(effective_skip_analytics),
            "analytics_only": bool(analytics_only),
            "run_measurement_analytics": bool(effective_run_measurement_analytics),
            "skip_measurement_analytics": bool(effective_skip_measurement_analytics),
            "measurement_analytics_only": bool(measurement_analytics_only),
            "download": bool(download),
            "latest_run": bool(latest_run),
        },
        "manifest": manifest_step,
        "database_import": {
            "status": database_status,
            "raw": raw_database_step,
            "enrich": enrich_database_step,
        },
        "qc": qc_step,
        "report": report_step,
        "refit": refit_step,
        "ai": ai_step,
        "analytics": analytics_step,
        "measurement_analytics": measurement_analytics_step,
        "download": download_step,
    }
    summary_json = _write_postprocess_summary(target_dir, summary_payload)

    result_payload: Dict[str, Any] = {"summary": str(summary_json)}
    if report_step.get("path"):
        result_payload["report"] = report_step["path"]
    if manifest_step.get("path"):
        result_payload["manifest"] = manifest_step["path"]
    if qc_step.get("json"):
        result_payload["qc_json"] = qc_step["json"]
    if qc_step.get("csv"):
        result_payload["qc_csv"] = qc_step["csv"]
    if ai_step.get("run_summary", {}).get("path"):
        result_payload["ai_run_summary"] = ai_step["run_summary"]["path"]
    if ai_step.get("anomaly_note", {}).get("path"):
        result_payload["ai_anomaly_note"] = ai_step["anomaly_note"]["path"]
    if analytics_step.get("dir"):
        result_payload["analytics_dir"] = analytics_step["dir"]
    if analytics_step.get("reports"):
        result_payload["analytics_reports"] = analytics_step["reports"]
    if measurement_analytics_step.get("dir"):
        result_payload["measurement_analytics_dir"] = measurement_analytics_step["dir"]
    if measurement_analytics_step.get("reports"):
        result_payload["measurement_analytics_reports"] = measurement_analytics_step["reports"]
    if isinstance(refit_step.get("runs"), list):
        result_payload["refit_runs"] = refit_step["runs"]
    if download_step["status"] == "completed":
        result_payload.update({key: value for key, value in download_step.items() if key != "status"})
    return result_payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline V1 sidecar post-processing entry")
    parser.add_argument("--run-dir", help="Completed V1/V2 run directory")
    parser.add_argument("--latest-run", action="store_true", help="Resolve the newest run_* directory under --latest-run-root")
    parser.add_argument("--latest-run-root", help="Root directory containing run_* directories")
    parser.add_argument("--summary", action="append", help="One or more combined analyzer summary files")
    parser.add_argument("--config", help="Optional JSON config file")
    parser.add_argument("--output-dir", help="Output directory for report and postprocess artifacts")
    parser.add_argument("--download", action="store_true", help="Explicitly publish coefficients after report generation")
    parser.add_argument("--import-db", action="store_true", help="Import the run directory into configured database storage")
    parser.add_argument("--dsn", help="Optional SQLAlchemy DSN for database import")
    parser.add_argument("--skip-qc", action="store_true", help="Skip QC regeneration from results.json")
    parser.add_argument("--skip-refit", action="store_true", help="Skip offline refit")
    parser.add_argument("--skip-ai", action="store_true", help="Disable AI-assisted QC explanation and offline AI summaries")
    parser.add_argument("--run-analytics", action="store_true", help="Explicitly run analytics export (enabled by default)")
    parser.add_argument("--skip-analytics", action="store_true", help="Skip analytics export")
    parser.add_argument("--analytics-only", action="store_true", help="Only generate manifest/database/analytics artifacts")
    parser.add_argument(
        "--run-measurement-analytics",
        action="store_true",
        help="Explicitly run measurement analytics export (enabled by default)",
    )
    parser.add_argument("--skip-measurement-analytics", action="store_true", help="Skip measurement analytics export")
    parser.add_argument(
        "--measurement-analytics-only",
        action="store_true",
        help="Only generate manifest/database/measurement analytics artifacts",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        run_from_cli(
            run_dir=args.run_dir,
            summary_paths=args.summary,
            config_path=args.config,
            output_dir=args.output_dir,
            download=bool(args.download),
            import_db=bool(args.import_db),
            dsn=args.dsn,
            skip_qc=bool(args.skip_qc),
            skip_refit=bool(args.skip_refit),
            skip_ai=bool(args.skip_ai),
            latest_run_root=args.latest_run_root,
            latest_run=bool(args.latest_run),
            run_analytics=True if args.run_analytics else True,
            skip_analytics=bool(args.skip_analytics),
            analytics_only=bool(args.analytics_only),
            run_measurement_analytics=True if args.run_measurement_analytics else True,
            skip_measurement_analytics=bool(args.skip_measurement_analytics),
            measurement_analytics_only=bool(args.measurement_analytics_only),
        )
    except Exception as exc:  # pragma: no cover
        raise DataParseError("v1_postprocess_runner", reason=str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
