from .analyzer_health import build_analyzer_health
from .control_charts import build_control_charts
from .drift_metrics import build_drift_metrics
from .fault_attribution import build_fault_attribution
from .point_kpis import build_point_kpis
from .run_kpis import build_run_kpis
from .traceability import build_traceability

MART_BUILDERS = {
    "run_kpis": build_run_kpis,
    "point_kpis": build_point_kpis,
    "drift_metrics": build_drift_metrics,
    "control_charts": build_control_charts,
    "analyzer_health": build_analyzer_health,
    "fault_attribution": build_fault_attribution,
    "traceability": build_traceability,
}

__all__ = [
    "MART_BUILDERS",
    "build_analyzer_health",
    "build_control_charts",
    "build_drift_metrics",
    "build_fault_attribution",
    "build_point_kpis",
    "build_run_kpis",
    "build_traceability",
]
