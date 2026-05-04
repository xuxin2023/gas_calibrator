from __future__ import annotations

import math
from typing import Any

from ...export import export_ratio_poly_report
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


def saturation_vapor_pressure_hpa(temp_c: float) -> float:
    return 6.112 * math.exp((17.67 * float(temp_c)) / (float(temp_c) + 243.5))


def dry_air_corrected_co2_ppm(
    *,
    cylinder_co2_ppm: float,
    pressure_p3_hpa: float,
    temp_c: float,
) -> float:
    if cylinder_co2_ppm is None or pressure_p3_hpa is None or temp_c is None:
        return cylinder_co2_ppm
    e_t = saturation_vapor_pressure_hpa(temp_c)
    if pressure_p3_hpa <= e_t:
        return cylinder_co2_ppm
    effective = float(cylinder_co2_ppm) * float(pressure_p3_hpa) / (float(pressure_p3_hpa) - e_t)
    return round(effective, 4)


class CoefficientService:
    """Coefficient report export wrapper for the current ratio-poly path."""

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def export_coefficient_report(self) -> dict[str, str]:
        coeff_cfg = getattr(self.context.config, "coefficients", None)
        if coeff_cfg is None:
            message = "Coefficient report skipped: coefficients config unavailable"
            self.host._log(message)
            return {"status": "skipped", "error": message}
        if not coeff_cfg.enabled or not coeff_cfg.auto_fit:
            return {"status": "skipped", "error": "coefficients disabled"}
        if str(coeff_cfg.model).strip().lower() != "ratio_poly_rt_p":
            message = f"Coefficient report skipped: unsupported model {coeff_cfg.model}"
            self.host._log(message)
            return {"status": "skipped", "error": message}
        workflow_cfg = getattr(self.context.config, "workflow", None)
        summary_alignment_cfg = getattr(workflow_cfg, "summary_alignment", {}) if workflow_cfg is not None else {}
        if not isinstance(summary_alignment_cfg, dict):
            summary_alignment_cfg = {}
        reference_on_aligned_rows = bool(summary_alignment_cfg.get("reference_on_aligned_rows", True))
        expected_analyzers: list[str] = []
        analyzer_reader = getattr(self.host, "_all_gas_analyzers", None)
        if callable(analyzer_reader):
            expected_analyzers = [
                str(label or "").strip().upper()
                for label, _, _ in analyzer_reader()
                if str(label or "").strip()
            ]
        try:
            output_path = export_ratio_poly_report(
                self.host.get_results(),
                out_dir=self.context.result_store.run_dir,
                coeff_cfg=coeff_cfg,
                expected_analyzers=expected_analyzers,
                reference_on_aligned_rows=reference_on_aligned_rows,
            )
        except Exception as exc:
            self.host._log(f"Coefficient report export failed: {exc}")
            return {"status": "error", "error": str(exc)}
        if output_path is None:
            message = "Coefficient report skipped: insufficient fit-ready samples"
            self.host._log(message)
            return {"status": "skipped", "error": message}
        self.host._remember_output_file(str(output_path))
        self.host._log(f"Coefficient report saved: {output_path}")
        return {"status": "ok", "path": str(output_path)}
