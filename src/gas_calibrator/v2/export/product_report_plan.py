from __future__ import annotations

from dataclasses import dataclass
from typing import Any


REPORT_FAMILY = "v2_product_report_family"


@dataclass(frozen=True)
class ProductReportTemplate:
    key: str
    title: str
    channel: str
    report_kind: str
    formal_calibration: bool
    per_device_output: bool = True
    enabled: bool = True
    implementation_status: str = "skeleton_defined"
    current_exporter: str = ""
    filename_stub: str = ""
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "channel": self.channel,
            "report_kind": self.report_kind,
            "formal_calibration": bool(self.formal_calibration),
            "per_device_output": bool(self.per_device_output),
            "enabled": bool(self.enabled),
            "implementation_status": self.implementation_status,
            "current_exporter": self.current_exporter,
            "filename_stub": self.filename_stub,
            "description": self.description,
        }


def build_product_report_templates(
    *,
    run_mode: str = "auto_calibration",
    route_mode: str = "h2o_then_co2",
) -> list[ProductReportTemplate]:
    normalized_run_mode = str(run_mode or "auto_calibration").strip().lower() or "auto_calibration"
    normalized_route_mode = str(route_mode or "h2o_then_co2").strip().lower() or "h2o_then_co2"
    include_co2 = normalized_route_mode != "h2o_only"
    include_h2o = normalized_route_mode != "co2_only"
    calibration_enabled = normalized_run_mode == "auto_calibration"
    templates: list[ProductReportTemplate] = []

    if include_co2:
        templates.append(
            ProductReportTemplate(
                key="co2_test_report",
                title="CO2 Test Report",
                channel="co2",
                report_kind="test",
                formal_calibration=False,
                current_exporter="storage.exporter + run artifacts",
                filename_stub="reports/co2_test/{analyzer}.xlsx",
                description="Per-device CO2 measurement/test worksheet family.",
            )
        )
        templates.append(
            ProductReportTemplate(
                key="co2_calibration_report",
                title="CO2 Calibration Report",
                channel="co2",
                report_kind="calibration",
                formal_calibration=True,
                enabled=calibration_enabled,
                implementation_status="partial_existing" if calibration_enabled else "gated_off",
                current_exporter="ratio_poly_report",
                filename_stub="reports/co2_calibration/{analyzer}.xlsx",
                description="Formal per-device CO2 calibration workbook. Only enabled in auto calibration mode.",
            )
        )

    if include_h2o:
        templates.append(
            ProductReportTemplate(
                key="h2o_test_report",
                title="H2O Test Report",
                channel="h2o",
                report_kind="test",
                formal_calibration=False,
                current_exporter="storage.exporter + run artifacts",
                filename_stub="reports/h2o_test/{analyzer}.xlsx",
                description="Per-device H2O measurement/test worksheet family.",
            )
        )
        templates.append(
            ProductReportTemplate(
                key="h2o_calibration_report",
                title="H2O Calibration Report",
                channel="h2o",
                report_kind="calibration",
                formal_calibration=True,
                enabled=calibration_enabled,
                implementation_status="first_exporter_available" if calibration_enabled else "gated_off",
                current_exporter="storage.exporter.export_h2o_calibration_reports",
                filename_stub="reports/h2o_calibration/{device}.json",
                description=(
                    "First per-device formal H2O calibration export. Currently lands as structured JSON while the "
                    "final workbook template is still under construction."
                ),
            )
        )

    return templates


def build_product_report_manifest(
    *,
    run_mode: str = "auto_calibration",
    route_mode: str = "h2o_then_co2",
) -> dict[str, Any]:
    templates = build_product_report_templates(run_mode=run_mode, route_mode=route_mode)
    return {
        "report_family": REPORT_FAMILY,
        "run_mode": str(run_mode or "auto_calibration"),
        "route_mode": str(route_mode or "h2o_then_co2"),
        "per_device_output": True,
        "template_keys": [item.key for item in templates],
        "template_count": len(templates),
        "per_device_output_dirs": {
            "co2_test": "reports/co2_test",
            "co2_calibration": "reports/co2_calibration",
            "h2o_test": "reports/h2o_test",
            "h2o_calibration": "reports/h2o_calibration",
        },
        "current_capabilities": [
            {
                "component": "storage.exporter",
                "status": "existing",
                "scope": "run-level summary/points/samples/qc raw exports",
            },
            {
                "component": "ratio_poly_report",
                "status": "existing_partial",
                "scope": "calibration-oriented workbook and quality analysis, not yet the final 4-template product family",
            },
            {
                "component": "storage.exporter.export_h2o_calibration_reports",
                "status": "first_real_exporter",
                "scope": "per-device H2O calibration JSON export with manifest linkage and mode gating",
            },
            {
                "component": "run_manifest",
                "status": "new_skeleton_hook",
                "scope": "records which product report templates apply to this run mode and route mode",
            },
        ],
        "templates": [item.to_dict() for item in templates],
        "notes": [
            "Only auto_calibration mode enables formal calibration reports.",
            "Measurement and experiment modes remain test-report only.",
            "This manifest defines report families and output paths without rewriting current exporters.",
        ],
    }
