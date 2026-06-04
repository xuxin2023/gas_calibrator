"""Inventory and classify V1.5 entrypoints.

The classifier is intentionally conservative. It does not decide whether a tool
is correct; it records what kind of entrypoint it appears to be so formal V1.5
work can avoid mixing production runners with diagnostics and one-off analysis.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


V1_5_TOOL_PREFIXES = (
    "run_v1_5_",
    "export_v1_5_",
    "prepare_v1_5_",
    "import_v1_5_",
    "query_v1_5_",
    "probe_v1_5_",
    "summarize_v1_5_",
    "collect_v1_5_",
    "migrate_v1_5_",
    "archive_v1_5_",
)


@dataclass(frozen=True)
class V15Entrypoint:
    path: str
    name: str
    artifact_type: str
    category: str
    stage: str
    formal_status: str
    risk_level: str
    opens_com_ports: bool
    controls_routes: bool
    writes_coefficients: bool
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict:
        return asdict(self)


def _rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _stage_from_name(name: str) -> str:
    lower = name.lower()
    if "serial_port" in lower:
        return "identity_and_serial_binding"
    if "pressure" in lower or "senco9" in lower or "pace" in lower:
        return "pressure_channel"
    if "temperature" in lower or "temp" in lower:
        return "temperature_channel"
    if "h2o" in lower or "dewpoint" in lower or "humidity" in lower:
        return "h2o_component"
    if "co2" in lower or "senco1" in lower or "senco3" in lower or "senco5" in lower:
        return "co2_component"
    if "evidence" in lower or "database" in lower or "db" in lower or "registry" in lower:
        return "evidence_database"
    if "report" in lower or "calibration_package" in lower:
        return "reporting"
    if "qc" in lower or "quality" in lower:
        return "qc_review"
    if "operation_console" in lower or "workbench" in lower or "review_surface" in lower:
        return "ui_review"
    if "full_flow" in lower or "formal_run_package" in lower:
        return "full_flow_orchestration"
    if "candidate" in lower or "coefficient" in lower or "fit" in lower:
        return "coefficient_review"
    return "general"


def _notes_for_name(name: str) -> list[str]:
    lower = name.lower()
    notes: list[str] = []
    if lower == "probe_v1_5_getco_component_snapshot":
        notes.append("formal precheck evidence; read-only GETCO1-9 and device-ID snapshot")
    elif "diagnostic" in lower or "probe" in lower or "tune" in lower:
        notes.append("diagnostic evidence only; not formal acceptance by default")
    if "sealed" in lower or "dynamic_pressure" in lower or "no_outp" in lower:
        notes.append("pressure/route engineering probe; keep outside formal CO2/H2O fit")
    if "controlled_write" in lower or "rollback" in lower:
        notes.append("requires explicit coefficient-write authorization and readback evidence")
    if "formal" in lower and "run_" in lower:
        notes.append("formal V1.5 entrypoint candidate; still requires operator authorization for real COM")
    if "serial_port" in lower:
        notes.append("COM port is transport only; analyzer identity remains MODE2 device ID")
    return notes


def classify_v1_5_entrypoint(path: Path, *, root: Path | None = None) -> V15Entrypoint:
    root = root or Path.cwd()
    rel_path = _rel(path, root)
    name = path.stem
    parts = set(path.parts)
    lower = name.lower()
    rel_lower = rel_path.lower()

    if "tests" in parts:
        artifact_type = "test"
        category = "test_gate"
        formal_status = "verification_only"
        risk_level = "none"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    elif "\\storage\\" in str(path).lower() or "/storage/" in rel_lower:
        artifact_type = "storage"
        category = "evidence_database"
        formal_status = "formal_support"
        risk_level = "offline"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    elif "\\v1_5\\" in str(path).lower() or "/v1_5/" in rel_lower:
        artifact_type = "library"
        if "qc_advanced" in rel_lower:
            category = "advanced_qc"
            formal_status = "formal_support"
        elif "orchestration" in rel_lower:
            category = "full_flow_orchestration"
            formal_status = "formal_support"
        elif "ui" in rel_lower or "review_surface" in lower:
            category = "ui_review"
            formal_status = "prototype_or_review_surface"
        elif "parameters" in rel_lower:
            category = "parameter_governance"
            formal_status = "formal_support"
        else:
            category = "v1_5_library"
            formal_status = "formal_support"
        risk_level = "offline"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    else:
        artifact_type = "tool"
        controls_routes = lower.startswith("run_v1_5_formal_") or any(
            token in lower for token in ("open_flow_sampling", "open_flow_queue", "h2o_open_flow", "co2_open_flow")
        )
        writes_coefficients = any(token in lower for token in ("controlled_write", "rollback", "neutral_controlled"))
        opens_com_ports = lower.startswith("run_v1_5_") or lower.startswith("probe_v1_5_")

        if lower == "run_v1_5_full_calibration_chain":
            category = "full_flow_orchestration"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
        elif "formal_evidence_sidecar" in lower or "formal_offline_review_chain" in lower:
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
        elif lower.startswith("archive_v1_5_"):
            category = "housekeeping_archive"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        elif writes_coefficients:
            category = "controlled_write"
            formal_status = "formal_but_manual_authorized"
            risk_level = "writes_device_coefficients"
        elif lower == "probe_v1_5_getco_component_snapshot":
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "real_com_or_route_risk"
        elif any(
            token in lower
            for token in ("diagnostic", "probe", "sealed", "dynamic_pressure", "no_outp", "tune", "extended_hold")
        ):
            category = "diagnostic_only"
            formal_status = "diagnostic_only"
            risk_level = "real_com_or_route_risk" if opens_com_ports else "offline"
        elif lower.startswith("run_v1_5_formal_") or lower == "run_v1_5_full_calibration_chain":
            category = "formal_runner"
            formal_status = "formal_entry_candidate"
            risk_level = "real_com_or_route_risk" if opens_com_ports or controls_routes else "offline"
        elif lower.startswith(
            (
                "prepare_v1_5_",
                "export_v1_5_",
                "import_v1_5_",
                "query_v1_5_",
                "migrate_v1_5_",
                "summarize_v1_5_",
            )
        ):
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        elif lower.startswith("collect_v1_5_"):
            category = "identity_and_serial_binding"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        else:
            category = "unclassified_v1_5_tool"
            formal_status = "needs_review"
            risk_level = "unknown"

    return V15Entrypoint(
        path=rel_path,
        name=name,
        artifact_type=artifact_type,
        category=category,
        stage=_stage_from_name(name),
        formal_status=formal_status,
        risk_level=risk_level,
        opens_com_ports=opens_com_ports,
        controls_routes=controls_routes,
        writes_coefficients=writes_coefficients,
        notes=tuple(_notes_for_name(name)),
    )


def discover_v1_5_entrypoints(root: Path) -> list[V15Entrypoint]:
    paths: list[Path] = []
    for base in ("src/gas_calibrator/tools", "src/gas_calibrator/v1_5", "src/gas_calibrator/storage", "tests"):
        folder = root / base
        if not folder.exists():
            continue
        for path in folder.rglob("*.py"):
            rel = _rel(path, root)
            name = path.stem
            if base == "tests" and not name.startswith("test_v1_5"):
                continue
            if base.endswith("tools") and not name.startswith(V1_5_TOOL_PREFIXES):
                continue
            if base.endswith("storage") and "v1_5_evidence" not in rel:
                continue
            paths.append(path)
    return [classify_v1_5_entrypoint(path, root=root) for path in sorted(paths)]


def summarize_entrypoints(entries: Iterable[V15Entrypoint]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for entry in entries:
        summary[entry.category] = summary.get(entry.category, 0) + 1
    return dict(sorted(summary.items()))
