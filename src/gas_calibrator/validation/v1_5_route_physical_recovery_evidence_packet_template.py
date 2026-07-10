"""Build offline templates for V1.5 route physical recovery evidence packets.

The template is intentionally offline. It does not open COM ports, control
pressure, control gas/water routes, connect PostgreSQL, or write analyzer
state. Its job is to make the next physical recovery evidence collection
explicit before the packet is reviewed by
``v1_5_route_physical_recovery_evidence_packet``.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "v1_5_route_physical_recovery_evidence_packet_template_v1"
PACKET_SCHEMA = "v1_5_route_physical_recovery_evidence_packet_v1"

SIDE_EFFECT_FALSE_KEYS = (
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "connects_postgresql",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "formal_release_allowed",
    "database_import_allowed",
)


@dataclass(frozen=True)
class RouteRecoveryCollectionStep:
    step_id: str
    packet_field: str
    physical_meaning: str
    required_evidence: str
    pass_condition: str
    recommended_artifacts: str
    collection_requires_real_hardware: bool
    forbidden_actions: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _root_cause_category_counts(root_cause: Mapping[str, Any]) -> dict[str, int]:
    manifest = root_cause.get("manifest") if isinstance(root_cause.get("manifest"), Mapping) else {}
    counts = manifest.get("category_counts")
    if isinstance(counts, Mapping):
        out: dict[str, int] = {}
        for key, value in counts.items():
            try:
                out[str(key)] = int(value)
            except Exception:
                out[str(key)] = 0
        return out
    out: dict[str, int] = {}
    findings = root_cause.get("findings") if isinstance(root_cause.get("findings"), list) else []
    for row in findings:
        if not isinstance(row, Mapping):
            continue
        category = str(row.get("category") or "").strip()
        if category:
            out[category] = out.get(category, 0) + 1
    return out


def _packet_template() -> dict[str, Any]:
    return {
        "schema": PACKET_SCHEMA,
        "dry_gas_dewpoint_recovery": {
            "status": "pending_review",
            "dewpoint_c": None,
            "dry_enough_threshold_c": -28.0,
            "tail_span_c": None,
            "max_tail_span_c": 0.5,
            "tail_slope_abs_c_per_s": None,
            "max_tail_slope_abs_c_per_s": 0.01,
            "route_or_dryer_checked": False,
            "evidence": {
                "dewpoint_trace": "",
                "route_or_dryer_check": "",
                "operator_note": "",
            },
        },
        "pace_vent_recovery": {
            "status": "pending_review",
            "vent_on_off_roundtrip_pass": False,
            "no_response_absent": False,
            "evidence": {
                "pace_vent_roundtrip_trace": "",
                "operator_note": "",
            },
        },
        "pressure_gauge_recovery": {
            "status": "pending_review",
            "readback_status": "pending_review",
            "absolute_pressure_source": "inl",
            "no_response_absent": False,
            "evidence": {
                "pressure_gauge_inl_trace": "",
                "operator_note": "",
            },
        },
        "accepted_manifest_review": {
            "status": "pending_review",
            "accepted_manifest_path": "",
            "supersedence_review_id": "",
            "note": "Required only before segmented/direct/retry evidence is made fit-eligible.",
        },
        "next_run_policy": {
            "fresh_canonical_queue": True,
            "mature_physical_baseline": "0613/0620/0621",
            "forbidden_surfaces_absent": True,
            "co2_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
            "h2o_entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
            "note": "Open the next continuous run from mature queue entrypoints only, not _handoff, 0624, worker, diagnostic, retry, or root migration surfaces.",
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _collection_steps() -> list[RouteRecoveryCollectionStep]:
    forbidden = "Do not write SN/device_code/SENCO, do not connect PostgreSQL, do not start CO2/H2O queue."
    return [
        RouteRecoveryCollectionStep(
            step_id="dry_gas_dewpoint_recovery",
            packet_field="dry_gas_dewpoint_recovery",
            physical_meaning="Prove the zero-gas route and dryer are dry enough before any formal CO2 dry/zero point.",
            required_evidence="Dewpoint trace after route/dryer check with stable tail.",
            pass_condition="status=pass; dewpoint_c <= -28 C; tail_span_c <= 0.5 C; tail_slope_abs_c_per_s <= 0.01; route_or_dryer_checked=true.",
            recommended_artifacts="dewpoint_trace.csv plus route/dryer check note or trace.",
            collection_requires_real_hardware=True,
            forbidden_actions=forbidden,
        ),
        RouteRecoveryCollectionStep(
            step_id="pace_vent_recovery",
            packet_field="pace_vent_recovery",
            physical_meaning="Prove PACE vent communication recovered after previous NO_RESPONSE failures.",
            required_evidence="PACE vent ON/OFF roundtrip trace with no NO_RESPONSE.",
            pass_condition="status=pass; vent_on_off_roundtrip_pass=true; no_response_absent=true.",
            recommended_artifacts="pace_vent_roundtrip.csv or reviewed IO trace.",
            collection_requires_real_hardware=True,
            forbidden_actions=forbidden,
        ),
        RouteRecoveryCollectionStep(
            step_id="pressure_gauge_recovery",
            packet_field="pressure_gauge_recovery",
            physical_meaning="Prove COM22 pressure gauge readback uses mature INL absolute pressure and is responsive.",
            required_evidence="Pressure gauge readback trace using INL absolute-pressure source.",
            pass_condition="status=pass; readback_status=pass; absolute_pressure_source=inl; no_response_absent=true.",
            recommended_artifacts="com22_inl_readback.csv or reviewed pressure trace.",
            collection_requires_real_hardware=True,
            forbidden_actions=forbidden,
        ),
        RouteRecoveryCollectionStep(
            step_id="fresh_canonical_queue_policy",
            packet_field="next_run_policy",
            physical_meaning="Prevent another segmented/direct/retry run from being treated as the next formal continuous run.",
            required_evidence="Reviewed plan that names the mature 0613/0620/0621 CO2/H2O queue entrypoints.",
            pass_condition="fresh_canonical_queue=true; baseline includes 0613/0620/0621; forbidden_surfaces_absent=true; canonical queue entrypoints named.",
            recommended_artifacts="fresh run plan JSON or reviewed operator note.",
            collection_requires_real_hardware=False,
            forbidden_actions="Do not reference _handoff, 0624, diagnostic, worker, retry, or root migration surfaces.",
        ),
        RouteRecoveryCollectionStep(
            step_id="accepted_manifest_review",
            packet_field="accepted_manifest_review",
            physical_meaning="Keep old segmented/direct/retry evidence out of fitting unless explicitly superseded.",
            required_evidence="Accepted manifest path and supersedence review id, if segmented evidence will be used.",
            pass_condition="status=pass; accepted_manifest_path is non-empty; supersedence_review_id is non-empty.",
            recommended_artifacts="accepted_manifest.csv and review note.",
            collection_requires_real_hardware=False,
            forbidden_actions="Do not make segmented evidence fit-eligible without accepted-manifest review.",
        ),
    ]


def build_v1_5_route_physical_recovery_evidence_packet_template(
    *,
    root_cause_audit_path: str | Path | None = None,
) -> dict[str, Any]:
    root_cause = _load_json(root_cause_audit_path)
    category_counts = _root_cause_category_counts(root_cause)
    packet = _packet_template()
    steps = _collection_steps()
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": "template_ready",
            "root_cause_audit_path": str(root_cause_audit_path or ""),
            "root_cause_category_counts": category_counts,
            "collection_step_count": len(steps),
            "packet_schema": PACKET_SCHEMA,
            "ready_for_validator": False,
            "reason": "Template fields must be replaced with reviewed physical evidence before validator pass is possible.",
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
        "collection_plan": [step.to_json() for step in steps],
        "recovery_evidence_packet_template": packet,
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = model.get("manifest") if isinstance(model.get("manifest"), Mapping) else {}
    lines = [
        "# V1.5 Route Physical Recovery Evidence Packet Template",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest.get('status')}`",
        f"- packet_schema: `{manifest.get('packet_schema')}`",
        "- boundary: offline template only; no COM, no pressure/route control, no writes, no PostgreSQL.",
        "",
        "## Collection Steps",
        "",
        "| step | packet field | pass condition | hardware |",
        "|---|---|---|---|",
    ]
    for row in model.get("collection_plan") or []:
        hardware = "yes" if row.get("collection_requires_real_hardware") else "no"
        lines.append(
            f"| `{row.get('step_id')}` | `{row.get('packet_field')}` | {row.get('pass_condition')} | {hardware} |"
        )
    lines.extend(
        [
            "",
            "## Usage",
            "",
            "1. Collect or review the physical evidence listed above.",
            "2. Replace the pending fields in `v1_5_route_physical_recovery_evidence_packet_template.json`.",
            "3. Run `export_v1_5_route_physical_recovery_evidence_packet.py` on the reviewed packet.",
            "4. Feed `v1_5_validated_route_physical_recovery_evidence.json` into route physical recovery readiness.",
            "",
            "This template is not real acceptance evidence and does not unlock a continuous run by itself.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_route_physical_recovery_evidence_packet_template(
    *,
    output_dir: str | Path,
    root_cause_audit_path: str | Path | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_route_physical_recovery_evidence_packet_template(
        root_cause_audit_path=root_cause_audit_path,
    )
    paths = {
        "manifest": out / "v1_5_route_physical_recovery_evidence_packet_template_manifest.json",
        "packet_template": out / "v1_5_route_physical_recovery_evidence_packet_template.json",
        "collection_plan": out / "v1_5_route_physical_recovery_evidence_collection_plan.csv",
        "markdown": out / "V1_5_ROUTE_PHYSICAL_RECOVERY_EVIDENCE_PACKET_TEMPLATE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["packet_template"].write_text(
        json.dumps(model["recovery_evidence_packet_template"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with paths["collection_plan"].open("w", newline="", encoding="utf-8") as handle:
        fieldnames = tuple(RouteRecoveryCollectionStep.__dataclass_fields__.keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(model["collection_plan"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_route_physical_recovery_evidence_packet_template",
    "write_v1_5_route_physical_recovery_evidence_packet_template",
]
