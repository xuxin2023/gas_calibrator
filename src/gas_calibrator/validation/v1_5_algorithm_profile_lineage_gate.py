"""Bind one V1.5 algorithm profile from bootstrap through fit-input semantics."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_mature_queue_inputs import (
    CO2_QUEUE_RUNNER,
    EXPECTED_COUNTS,
    H2O_QUEUE_RUNNER,
)

SCHEMA = "v1_5_algorithm_profile_lineage_gate_v1"
PROFILE_CONTRACTS = {
    "legacy_ratio_production": {
        "algorithm_mode": "legacy_ratio_R",
        "co2_fit_input": "R_CO2_with_chamber_temperature_terms",
        "h2o_fit_input": "R_H2O_with_chamber_temperature_terms",
        "r0_required": False,
    },
    "absorption_ratio_shadow": {
        "algorithm_mode": "absorption_ratio_A",
        "co2_fit_input": "A_CO2=-ln(R_CO2/R0_CO2(T))/(P_kPa/100)",
        "h2o_fit_input": "A_H2O=-ln(R_H2O/R0_H2O(T))/(P_kPa/100)",
        "r0_required": True,
    },
}


def _load(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def _point_identities(route: str, rows: Sequence[Mapping[str, Any]]) -> list[tuple[float, ...]]:
    identities: list[tuple[float, ...]] = []
    try:
        for row in rows:
            if route == "co2":
                identities.append(
                    (float(row["temp_c"]), float(row["source_nominal_ppm"]))
                )
            else:
                identities.append(
                    (
                        float(row["temp_c"]),
                        float(row["hgen_temp_c"]),
                        float(row["hgen_rh_pct"]),
                    )
                )
    except (KeyError, TypeError, ValueError):
        return []
    return identities


def _same_path(left: Any, right: Any) -> bool:
    if not left or not right:
        return False
    return Path(str(left)).resolve() == Path(str(right)).resolve()


def _check(name: str, reasons: Sequence[str], details: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "check": name,
        "status": "pass" if not reasons else "blocker",
        "reasons": list(dict.fromkeys(reasons)),
        "details": dict(details),
    }


def _route_check(
    *,
    route: str,
    bootstrap: Mapping[str, Any],
    queue_inputs: Mapping[str, Any],
    summary: Mapping[str, Any],
    manifest_path: Path,
    expected_count: int,
    expected_runner: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    queue_key = f"{route}_queue_csv"
    sha_key = f"{route}_queue_sha256"
    runner_key = f"{route}_queue_runner"
    queue_path = Path(str(queue_inputs.get(queue_key) or ""))
    queue_rows = _read_csv(queue_path)
    manifest_rows = _read_csv(manifest_path)
    if not _same_path(bootstrap.get(queue_key), queue_inputs.get(queue_key)):
        reasons.append(f"{route}_queue_path_mismatch")
    observed_sha = _sha(queue_path)
    if not observed_sha or observed_sha != str(queue_inputs.get(sha_key) or ""):
        reasons.append(f"{route}_queue_file_sha_mismatch")
    if str(bootstrap.get(sha_key) or "") != str(queue_inputs.get(sha_key) or ""):
        reasons.append(f"{route}_bootstrap_queue_sha_mismatch")
    if str(queue_inputs.get(runner_key) or "") != expected_runner:
        reasons.append(f"{route}_mature_runner_mismatch")
    if int(queue_inputs.get(f"{route}_point_count") or 0) != expected_count:
        reasons.append(f"{route}_queue_input_count_mismatch")
    queue_identities = _point_identities(route, queue_rows)
    manifest_identities = _point_identities(route, manifest_rows)
    if len(manifest_rows) != expected_count:
        reasons.append(f"{route}_point_manifest_count_mismatch")
    if not manifest_identities or manifest_identities != queue_identities:
        reasons.append(f"{route}_point_manifest_identity_mismatch")
    if len(set(manifest_identities)) != len(manifest_identities):
        reasons.append(f"{route}_point_manifest_duplicate_identity")
    if any(str(row.get("status") or "").lower() != "ok" for row in manifest_rows):
        reasons.append(f"{route}_point_manifest_has_non_ok_status")
    if not summary:
        reasons.append(f"{route}_queue_summary_missing")
    else:
        if not _same_path(summary.get("queue_csv"), queue_path):
            reasons.append(f"{route}_summary_queue_path_mismatch")
        if int(summary.get("selected_points") or 0) != expected_count:
            reasons.append(f"{route}_summary_selected_count_mismatch")
        if summary.get("dry_run") is not False:
            reasons.append(f"{route}_summary_is_not_real_acquisition")
        if int(summary.get("ok_points") or 0) != expected_count:
            reasons.append(f"{route}_summary_ok_count_mismatch")
        if int(summary.get("failed_points") or 0) != 0:
            reasons.append(f"{route}_summary_has_failed_points")
    return _check(
        f"{route}_queue_lineage",
        reasons,
        {
            "queue_csv": str(queue_path),
            "queue_sha256": observed_sha,
            "expected_point_count": expected_count,
            "runner": queue_inputs.get(runner_key),
            "summary_status": summary.get("overall_status") or summary.get("status"),
            "point_manifest": str(manifest_path),
            "point_manifest_count": len(manifest_rows),
        },
    )


def _r0_check(
    *,
    component: str,
    source: Path | None,
    profile_id: str,
    profile_sha256: str,
    required: bool,
) -> dict[str, Any]:
    if not required:
        return _check(
            f"{component}_r0_model_lineage",
            (),
            {"required": False, "contract": "legacy_ratio_does_not_consume_R0_T"},
        )
    payload = _load(source) if source else {}
    reasons: list[str] = []
    if not payload:
        reasons.append(f"{component}_r0_model_missing")
    if str(payload.get("profile_id") or "") != profile_id:
        reasons.append(f"{component}_r0_profile_mismatch")
    if str(payload.get("profile_sha256") or "") != profile_sha256:
        reasons.append(f"{component}_r0_profile_sha_mismatch")
    if str(payload.get("component") or "").lower() != component:
        reasons.append(f"{component}_r0_component_mismatch")
    if str(payload.get("status") or "").lower() not in {"pass", "ready"}:
        reasons.append(f"{component}_r0_status_not_pass")
    expected_variable = "R0_CO2(T)" if component == "co2" else "R0_H2O(T)"
    if str(payload.get("model_variable") or "") != expected_variable:
        reasons.append(f"{component}_r0_model_variable_mismatch")
    for key in ("opens_com_ports", "writes_coefficients", "connects_postgresql"):
        if payload.get(key) is not False:
            reasons.append(f"{component}_r0_{key}_must_be_false")
    return _check(
        f"{component}_r0_model_lineage",
        reasons,
        {"required": True, "source": str(source or ""), "model_variable": expected_variable},
    )


def build_v1_5_algorithm_profile_lineage_gate(
    *,
    bootstrap_json: str | Path,
    queue_inputs_json: str | Path,
    co2_queue_summary_json: str | Path,
    h2o_queue_summary_json: str | Path,
    co2_queue_manifest_csv: str | Path,
    h2o_queue_manifest_csv: str | Path,
    co2_r0_model_json: str | Path | None = None,
    h2o_r0_model_json: str | Path | None = None,
) -> dict[str, Any]:
    bootstrap_path = Path(bootstrap_json).resolve()
    queue_inputs_path = Path(queue_inputs_json).resolve()
    bootstrap = _load(bootstrap_path)
    queue_inputs = _load(queue_inputs_path)
    co2_summary = _load(Path(co2_queue_summary_json).resolve())
    h2o_summary = _load(Path(h2o_queue_summary_json).resolve())
    profile_id = str(bootstrap.get("algorithm_profile_id") or "")
    contract = PROFILE_CONTRACTS.get(profile_id, {})
    checks: list[dict[str, Any]] = []

    identity_reasons: list[str] = []
    if profile_id not in EXPECTED_COUNTS:
        identity_reasons.append("bootstrap_algorithm_profile_unknown")
    if str(queue_inputs.get("profile_id") or "") != profile_id:
        identity_reasons.append("queue_input_profile_id_mismatch")
    profile_sha = str(bootstrap.get("algorithm_profile_snapshot_sha256") or "")
    if not profile_sha or str(queue_inputs.get("profile_sha256") or "") != profile_sha:
        identity_reasons.append("queue_input_profile_sha_mismatch")
    if str(queue_inputs.get("algorithm_mode") or "") != str(contract.get("algorithm_mode") or ""):
        identity_reasons.append("queue_input_algorithm_mode_mismatch")
    if queue_inputs.get("profile_declared_queue_source_is_not_consumed") is not True:
        identity_reasons.append("profile_queue_source_bypass_contract_missing")
    checks.append(
        _check(
            "bootstrap_profile_identity",
            identity_reasons,
            {
                "profile_id": profile_id,
                "profile_sha256": profile_sha,
                "algorithm_mode": contract.get("algorithm_mode"),
            },
        )
    )

    co2_count, h2o_count = EXPECTED_COUNTS.get(profile_id, (0, 0))
    checks.append(
        _route_check(
            route="co2",
            bootstrap=bootstrap,
            queue_inputs=queue_inputs,
            summary=co2_summary,
            manifest_path=Path(co2_queue_manifest_csv).resolve(),
            expected_count=co2_count,
            expected_runner=CO2_QUEUE_RUNNER,
        )
    )
    checks.append(
        _route_check(
            route="h2o",
            bootstrap=bootstrap,
            queue_inputs=queue_inputs,
            summary=h2o_summary,
            manifest_path=Path(h2o_queue_manifest_csv).resolve(),
            expected_count=h2o_count,
            expected_runner=H2O_QUEUE_RUNNER,
        )
    )
    r0_required = bool(contract.get("r0_required"))
    checks.append(
        _r0_check(
            component="co2",
            source=Path(co2_r0_model_json).resolve() if co2_r0_model_json else None,
            profile_id=profile_id,
            profile_sha256=profile_sha,
            required=r0_required,
        )
    )
    checks.append(
        _r0_check(
            component="h2o",
            source=Path(h2o_r0_model_json).resolve() if h2o_r0_model_json else None,
            profile_id=profile_id,
            profile_sha256=profile_sha,
            required=r0_required,
        )
    )

    blocker_count = sum(row["status"] == "blocker" for row in checks)
    fit_input_contract = {
        "profile_id": profile_id,
        "profile_sha256": profile_sha,
        "algorithm_mode": contract.get("algorithm_mode", ""),
        "co2_fit_input": contract.get("co2_fit_input", ""),
        "h2o_fit_input": contract.get("h2o_fit_input", ""),
        "pressure_normalization": "P_kPa/100" if r0_required else "pressure_separate_mature_ratio_contract",
        "r0_required": r0_required,
        "temperature_source": "per_analyzer_chamber_T1",
        "co2_zero_and_h2o_dry_anchor_are_separate": True,
    }
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "overall_status": "pass" if blocker_count == 0 else "blocked",
        "blocker_count": blocker_count,
        "fit_input_allowed": blocker_count == 0,
        "fit_input_contract": fit_input_contract,
        "checks": checks,
        "source_paths": {
            "bootstrap_json": str(bootstrap_path),
            "queue_inputs_json": str(queue_inputs_path),
            "co2_queue_summary_json": str(Path(co2_queue_summary_json).resolve()),
            "h2o_queue_summary_json": str(Path(h2o_queue_summary_json).resolve()),
            "co2_queue_manifest_csv": str(Path(co2_queue_manifest_csv).resolve()),
            "h2o_queue_manifest_csv": str(Path(h2o_queue_manifest_csv).resolve()),
            "co2_r0_model_json": str(Path(co2_r0_model_json).resolve()) if co2_r0_model_json else "",
            "h2o_r0_model_json": str(Path(h2o_r0_model_json).resolve()) if h2o_r0_model_json else "",
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def write_v1_5_algorithm_profile_lineage_gate(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output / "v1_5_algorithm_profile_lineage_gate.json",
        "checks": output / "v1_5_algorithm_profile_lineage_checks.csv",
        "markdown": output / "V1_5_ALGORITHM_PROFILE_LINEAGE_GATE.md",
    }
    paths["json"].write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with paths["checks"].open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("check", "status", "reasons", "details"))
        writer.writeheader()
        for row in model.get("checks", []):
            writer.writerow(
                {
                    "check": row.get("check"),
                    "status": row.get("status"),
                    "reasons": ";".join(row.get("reasons") or []),
                    "details": json.dumps(row.get("details") or {}, ensure_ascii=False),
                }
            )
    contract = model.get("fit_input_contract") or {}
    lines = [
        "# V1.5 Algorithm Profile Lineage Gate",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- profile_id: `{contract.get('profile_id')}`",
        f"- CO2 fit input: `{contract.get('co2_fit_input')}`",
        f"- H2O fit input: `{contract.get('h2o_fit_input')}`",
        f"- R0(T) required: `{contract.get('r0_required')}`",
        "- This is offline/no-write evidence and never authorizes release or database import.",
    ]
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths


__all__ = [
    "PROFILE_CONTRACTS",
    "SCHEMA",
    "build_v1_5_algorithm_profile_lineage_gate",
    "write_v1_5_algorithm_profile_lineage_gate",
]
