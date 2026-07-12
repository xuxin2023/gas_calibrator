"""Bind exact historical route roots to the mature V1.5 route contract.

The binder is offline and fail closed.  It does not infer provenance from a
directory label.  A reviewed family is emitted only when the exact queue root,
closed queue artifacts, point sidecars, samples, component quality evidence,
and the 0613/0620/0621 contracts agree.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_historical_route_attestation_binder_v1"
ATTESTATION_SCHEMA = "v1_5_historical_route_baseline_attestation_v1"
MATURE_CONTRACT = "0613_fit_0620_0621_route"
FIT_BASELINE = "0613"

_FORBIDDEN_PROVENANCE = (
    (re.compile(r"(?:^|[\\/_\-.])(?:20260624|0624)(?:[\\/_\-.]|$)", re.I), "0624_source_forbidden"),
    (re.compile(r"migration", re.I), "migration_source_forbidden"),
    (re.compile(r"segmented", re.I), "segmented_source_forbidden"),
    (re.compile(r"(?:^|[\\/_\-.])retry(?:[\\/_\-.]|$)", re.I), "retry_source_forbidden"),
    (re.compile(r"(?:^|[\\/_\-.])direct(?:[\\/_\-.]|$)", re.I), "direct_source_forbidden"),
    (re.compile(r"(?:^|[\\/_\-.])recovery(?:[\\/_\-.]|$)", re.I), "recovery_source_forbidden"),
    (re.compile(r"diagnostic", re.I), "diagnostic_source_forbidden"),
    (re.compile(r"(?:^|[\\/_\-.])worker(?:[\\/_\-.]|$)", re.I), "worker_source_forbidden"),
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "ok", "pass", "reviewed"}


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip()) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _valid_reviewed_at(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return parsed.tzinfo is not None


def _same_path(left: Any, right: Any) -> bool:
    try:
        return Path(str(left)).resolve() == Path(str(right)).resolve()
    except (OSError, ValueError):
        return False


def _profiles(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = _read_json(path)
    return {str(row.get("profile_id")): dict(row) for row in payload.get("profiles", [])}


def _expected_points(profile: Mapping[str, Any], route_kind: str) -> list[tuple[float, ...]]:
    if route_kind == "co2":
        route = dict(profile.get("co2_route") or {})
        points = [
            (float(temp), float(ppm))
            for temp, values in (route.get("temperature_plan") or {}).items()
            for ppm in values
        ]
        for row in (route.get("supplement_policy") or {}).get(
            "required_new_algorithm_supplemental_gas_points", []
        ):
            points.append((float(row["temperature_c"]), float(row["co2_ppm"])))
        return sorted(set(points), key=lambda item: (-item[0], item[1]))
    route = dict(profile.get("h2o_route") or {})
    plan = route.get("wet_temperature_plan") or route.get("temperature_plan") or {}
    points: list[tuple[float, ...]] = []
    for temp, values in plan.items():
        for value in values:
            match = re.fullmatch(r"HGEN(-?\d+(?:\.\d+)?)C_(-?\d+(?:\.\d+)?)RH", str(value))
            if not match:
                raise ValueError(f"Invalid H2O profile point: {value}")
            points.append((float(temp), float(match.group(1)), float(match.group(2))))
    for row in route.get("required_new_algorithm_supplemental_wet_points", []):
        hgen = re.fullmatch(r"HGEN(-?\d+(?:\.\d+)?)C", str(row["humidity_generator"]))
        if not hgen:
            raise ValueError(f"Invalid H2O supplement: {row}")
        points.append(
            (
                float(row["temperature_c"]),
                float(hgen.group(1)),
                float(row["relative_humidity_pct"]),
            )
        )
    return sorted(set(points), key=lambda item: (item[0], item[1], item[2]))


def _manifest_points(rows: Sequence[Mapping[str, Any]], route_kind: str) -> list[tuple[float, ...]]:
    if route_kind == "co2":
        return [
            (float(row["temp_c"]), float(row["source_nominal_ppm"]))
            for row in rows
            if row.get("temp_c") not in (None, "") and row.get("source_nominal_ppm") not in (None, "")
        ]
    return [
        (float(row["temp_c"]), float(row["hgen_temp_c"]), float(row["hgen_rh_pct"]))
        for row in rows
        if all(row.get(key) not in (None, "") for key in ("temp_c", "hgen_temp_c", "hgen_rh_pct"))
    ]


def _declared_point_count(profile: Mapping[str, Any], route_kind: str) -> int | None:
    route = dict(profile.get(f"{route_kind}_route") or {})
    if route_kind == "co2":
        value = route.get("production_candidate_point_count_with_supplements")
        if value is None:
            value = route.get("formal_point_count")
    else:
        value = route.get("production_candidate_wet_point_count_with_supplements")
        if value is None:
            value = route.get("formal_wet_point_count", route.get("formal_point_count"))
    number = _number(value)
    return int(number) if number is not None and number.is_integer() else None


def _contract_reasons(mature: Mapping[str, Any], automation: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    route_manifest = mature.get("manifest") or {}
    route_contract = route_manifest.get("mature_route_contract") or {}
    auto_manifest = automation.get("manifest") or {}
    if route_manifest.get("status") != "pass" or route_manifest.get("blocker_count") != 0:
        reasons.append("mature_route_contract_not_pass")
    if route_contract.get("route_behavior") != "preserve_mature_v1_5_0620_route_timing_and_quality_gates":
        reasons.append("mature_route_behavior_mismatch")
    if route_contract.get("co2_runner") != "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue":
        reasons.append("mature_co2_runner_mismatch")
    if route_contract.get("h2o_runner") != "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue":
        reasons.append("mature_h2o_runner_mismatch")
    if auto_manifest.get("status") != "pass" or auto_manifest.get("blocker_count") != 0:
        reasons.append("automation_contract_not_pass")
    if auto_manifest.get("mature_fitting_baseline") != "0613-style V1.5 fitting method":
        reasons.append("0613_fitting_contract_missing")
    if auto_manifest.get("mature_physical_baseline") != "0620/0621 mature physical execution path":
        reasons.append("0620_0621_physical_contract_missing")
    return reasons


def _find_single(root: Path, name: str) -> tuple[Path | None, str | None]:
    paths = sorted(path for path in root.rglob(name) if path.is_file())
    if len(paths) == 1:
        return paths[0], None
    return None, f"{name}_{'missing' if not paths else 'ambiguous'}"


def _sidecar_reasons(sidecar: Mapping[str, Any], route_kind: str) -> list[str]:
    reasons: list[str] = []
    expected_schema = (
        "v1_5_formal_open_flow_sidecar_v0"
        if route_kind == "co2"
        else "v1_5_formal_h2o_open_flow_sidecar_v0"
    )
    if sidecar.get("schema_version") != expected_schema:
        reasons.append("point_sidecar_schema_mismatch")
    if sidecar.get("pace_mode") != "continuous_atmosphere_hold":
        reasons.append("point_pace_mode_not_continuous_atmosphere_hold")
    expected_scope = (
        "open_flow_purge_and_sampling_only"
        if route_kind == "co2"
        else "h2o_open_flow_purge_and_sampling_only"
    )
    if sidecar.get("continuous_atmosphere_hold_scope") != expected_scope:
        reasons.append("point_continuous_atmosphere_hold_scope_mismatch")
    if sidecar.get("route_open_until_sample_end") is not True:
        reasons.append("point_route_not_open_until_sample_end")
    if sidecar.get("analyzer_acquisition_policy") != "active_mode2_stream_1hz_ftd01_controlled":
        reasons.append("point_analyzer_acquisition_not_mature_1hz")
    if _number(sidecar.get("analyzer_stream_native_hz")) != 1.0:
        reasons.append("point_analyzer_stream_not_1hz")
    if _number(sidecar.get("formal_sample_anchor_interval_s")) != 1.0:
        reasons.append("point_sample_anchor_not_1s")
    if sidecar.get("writes_senco") is not False or sidecar.get("writes_device_id") is not False:
        reasons.append("point_sidecar_write_boundary_invalid")
    if sidecar.get("sealed_pressure_control") is not False:
        reasons.append("point_sealed_pressure_control_forbidden")
    if route_kind == "h2o":
        if sidecar.get("h2o_hgen_shutdown_policy") != "queue_managed_keep_running_between_points":
            reasons.append("h2o_hgen_queue_continuity_missing")
        if sidecar.get("h2o_open_flow_wait_contract") != "v1_5_dewpoint_tail_h2o_ratio_with_pressure_diagnostic_only":
            reasons.append("h2o_wait_contract_mismatch")
        if sidecar.get("h2o_pressure_presample_policy") != "skip":
            reasons.append("h2o_pressure_presample_policy_mismatch")
    else:
        if sidecar.get("gas_route_dewpoint_gate_enabled") is not True:
            reasons.append("co2_dewpoint_gate_not_enabled")
        if sidecar.get("gas_route_dewpoint_gate_policy") != "reject":
            reasons.append("co2_dewpoint_gate_policy_mismatch")
        if _number(sidecar.get("gas_route_dewpoint_gate_dry_enough_c")) != -28.0:
            reasons.append("co2_dewpoint_gate_threshold_mismatch")
    actual = _number(sidecar.get("actual_purge_s"))
    minimum = _number(sidecar.get("minimum_purge_s"))
    if actual is None or minimum is None or actual < minimum:
        reasons.append("point_actual_purge_below_minimum")
    return reasons


def _add_evidence(inventory: list[dict[str, Any]], root_key: str, role: str, path: Path) -> None:
    inventory.append(
        {
            "root_key": root_key,
            "role": role,
            "path": str(path.resolve()),
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
    )


def build_v1_5_historical_route_attestation_binder(
    *,
    historical_replay_evidence_json: str | Path,
    algorithm_profile_path: str | Path,
    mature_route_contract_json: str | Path,
    automation_control_contract_json: str | Path,
    reviewer: str,
    reviewed_at: str,
) -> dict[str, Any]:
    replay_path = Path(historical_replay_evidence_json).resolve()
    profile_path = Path(algorithm_profile_path).resolve()
    mature_path = Path(mature_route_contract_json).resolve()
    automation_path = Path(automation_control_contract_json).resolve()
    replay = _read_json(replay_path)
    profiles = _profiles(profile_path)
    contract_reasons = _contract_reasons(_read_json(mature_path), _read_json(automation_path))
    root_results: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    families: list[dict[str, Any]] = []
    root_key_counts: dict[str, int] = {}
    for row in replay.get("evidence_roots", []):
        key = f"{str(row.get('family_id') or '').strip()}:{str(row.get('route_kind') or '').strip().lower()}"
        root_key_counts[key] = root_key_counts.get(key, 0) + 1

    def block(root_key: str, root: Mapping[str, Any], code: str, detail: str = "") -> None:
        blockers.append(
            {
                "root_key": root_key,
                "family_id": str(root.get("family_id") or ""),
                "route_kind": str(root.get("route_kind") or ""),
                "root_path": str(root.get("root_path") or ""),
                "code": code,
                "detail": detail,
            }
        )

    evidence_roots = list(replay.get("evidence_roots") or [])
    if not evidence_roots:
        blockers.append(
            {
                "root_key": "",
                "family_id": "",
                "route_kind": "",
                "root_path": "",
                "code": "historical_evidence_roots_missing",
                "detail": "No exact historical route roots were supplied.",
            }
        )
    for root_row in evidence_roots:
        root = dict(root_row)
        family_id = str(root.get("family_id") or "").strip()
        route_kind = str(root.get("route_kind") or "").strip().lower()
        root_path_text = str(root.get("root_path") or "").strip()
        root_path = Path(root_path_text).resolve() if root_path_text else Path("__missing_route_root__").resolve()
        root_key = f"{family_id}:{route_kind}"
        before = len(blockers)
        if root_key_counts.get(root_key, 0) > 1:
            block(root_key, root, "duplicate_family_route_root")
        if route_kind not in {"co2", "h2o"}:
            block(root_key, root, "route_kind_invalid")
        if not root_path_text:
            block(root_key, root, "root_path_missing")
        elif not root_path.is_dir():
            block(root_key, root, "root_path_missing")
        for reason in contract_reasons:
            block(root_key, root, reason)
        if not reviewer.strip():
            block(root_key, root, "reviewer_missing")
        if not _valid_reviewed_at(reviewed_at):
            block(root_key, root, "reviewed_at_invalid_or_timezone_missing")
        profile_id = str(root.get("algorithm_profile_id") or "")
        profile = profiles.get(profile_id)
        if not profile:
            block(root_key, root, "algorithm_profile_missing", profile_id)
        provenance_text = " ".join((str(root_path), family_id, str(root.get("label") or "")))
        if root_path_text and root_path.is_dir():
            manifest_path, manifest_error = _find_single(root_path, "queue_manifest.csv")
            summary_path, summary_error = _find_single(root_path, "queue_summary.json")
            if manifest_error:
                block(root_key, root, manifest_error)
            if summary_error:
                block(root_key, root, summary_error)
            summary: dict[str, Any] = {}
            manifest_rows: list[dict[str, str]] = []
            if summary_path:
                summary = _read_json(summary_path)
                _add_evidence(inventory, root_key, "queue_summary", summary_path)
                provenance_text += " " + " ".join(
                    str(summary.get(key) or "")
                    for key in ("queue_run_id", "config_path", "queue_csv", "output_dir")
                )
            if manifest_path:
                manifest_rows = _read_csv(manifest_path)
                _add_evidence(inventory, root_key, "queue_manifest", manifest_path)
            for pattern, code in _FORBIDDEN_PROVENANCE:
                if pattern.search(provenance_text):
                    block(root_key, root, code, provenance_text)
            expected = _expected_points(profile, route_kind) if profile and route_kind in {"co2", "h2o"} else []
            observed = _manifest_points(manifest_rows, route_kind) if route_kind in {"co2", "h2o"} else []
            declared_count = _declared_point_count(profile, route_kind) if profile and route_kind in {"co2", "h2o"} else None
            if not expected or declared_count != len(expected):
                block(
                    root_key,
                    root,
                    "algorithm_profile_route_plan_invalid",
                    f"declared={declared_count};generated={len(expected)}",
                )
            if len(manifest_rows) != len(expected):
                block(root_key, root, "queue_point_count_mismatch", f"expected={len(expected)};observed={len(manifest_rows)}")
            if sorted(set(observed)) != sorted(set(expected)) or len(observed) != len(set(observed)):
                block(root_key, root, "queue_point_set_or_uniqueness_mismatch")
            if observed != expected:
                block(root_key, root, "queue_point_order_mismatch")
            for row in manifest_rows:
                point_id = str(row.get("point_run_id") or "")
                if row.get("status") != "ok" or str(row.get("returncode") or "") != "0":
                    block(root_key, root, "queue_point_not_ok", point_id)
                if any(pattern.search(point_id) for pattern, _ in _FORBIDDEN_PROVENANCE):
                    block(root_key, root, "queue_point_forbidden_lineage", point_id)
            readiness = summary.get("formal_route_readiness") or {}
            expected_count = len(expected)
            expected_summary_schema = (
                "v1_5_co2_open_flow_queue_v0"
                if route_kind == "co2"
                else "v1_5_h2o_open_flow_queue_v0"
            )
            if summary.get("schema_version") != expected_summary_schema:
                block(root_key, root, "queue_summary_schema_mismatch")
            if (
                summary.get("dry_run") is not False
                or summary.get("no_write") is not True
                or summary.get("writes_senco") is not False
                or summary.get("writes_device_id") is not False
                or summary.get("hard_failure") is not False
                or _number(summary.get("selected_points")) != float(expected_count)
                or _number(summary.get("ok_points")) != float(expected_count)
                or _number(summary.get("failed_points")) != 0.0
            ):
                block(root_key, root, "queue_summary_not_closed_clean_nowrite")
            if readiness.get("status") != "pass" or readiness.get("ok") is not True:
                block(root_key, root, "formal_route_readiness_not_pass")
            if summary and not _same_path(summary.get("output_dir"), root_path):
                block(root_key, root, "queue_summary_output_root_mismatch")
            source_paths: dict[str, Path] = {}
            for role, key in (("queue_source", "queue_csv"), ("runtime_config", "config_path")):
                raw_path = str(summary.get(key) or "").strip()
                source_path = Path(raw_path).resolve() if raw_path else Path("__missing_source__").resolve()
                if not raw_path or not source_path.is_file():
                    block(root_key, root, f"{role}_missing", raw_path)
                else:
                    source_paths[role] = source_path
                    _add_evidence(inventory, root_key, role, source_path)
            queue_source = source_paths.get("queue_source")
            if queue_source:
                source_points = _manifest_points(_read_csv(queue_source), route_kind)
                if sorted(set(source_points)) != sorted(set(expected)) or len(source_points) != len(set(source_points)):
                    block(root_key, root, "queue_source_point_plan_mismatch")
            for row in manifest_rows:
                point_id = str(row.get("point_run_id") or "")
                command = str(row.get("command") or "")
                if not command or str(root_path).lower() not in command.lower():
                    block(root_key, root, "point_command_output_root_mismatch", point_id)
                config_path = str(summary.get("config_path") or "")
                if not config_path or config_path.lower() not in command.lower():
                    block(root_key, root, "point_command_runtime_config_mismatch", point_id)
                point_dir = root_path / point_id
                if not point_dir.is_dir():
                    block(root_key, root, "point_directory_missing", point_id)
                    continue
                sidecar_name = (
                    "formal_open_flow_sidecar_metadata.json"
                    if route_kind == "co2"
                    else "formal_h2o_open_flow_sidecar_metadata.json"
                )
                sidecar_path = point_dir / sidecar_name
                samples_path = point_dir / "samples_machine_readable.csv"
                quality_path = point_dir / "formal_open_flow_data_quality_by_analyzer.csv"
                for role, path in (
                    ("point_sidecar", sidecar_path),
                    ("point_samples", samples_path),
                    ("point_component_quality", quality_path),
                ):
                    if not path.is_file() or path.stat().st_size == 0:
                        block(root_key, root, f"{role}_missing", point_id)
                    else:
                        _add_evidence(inventory, root_key, role, path)
                if sidecar_path.is_file():
                    sidecar = _read_json(sidecar_path)
                    if sidecar.get("run_id") != point_id:
                        block(root_key, root, "point_sidecar_run_id_mismatch", point_id)
                    for reason in _sidecar_reasons(sidecar, route_kind):
                        block(root_key, root, reason, point_id)
                if samples_path.is_file() and not _read_csv(samples_path):
                    block(root_key, root, "point_samples_empty", point_id)
                if quality_path.is_file() and not _read_csv(quality_path):
                    block(root_key, root, "point_component_quality_empty", point_id)
        root_blockers = blockers[before:]
        status = "reviewed" if not root_blockers else "blocked"
        root_results.append(
            {
                "root_key": root_key,
                "family_id": family_id,
                "route_kind": route_kind,
                "algorithm_profile_id": profile_id,
                "root_path": str(root_path),
                "status": status,
                "blocker_count": len(root_blockers),
                "blocker_codes": sorted({row["code"] for row in root_blockers}),
            }
        )
        if status == "reviewed":
            families.append(
                {
                    "family_id": family_id,
                    "route_kind": route_kind,
                    "root_path": str(root_path),
                    "route_baseline": "0620" if route_kind == "co2" else "0621",
                    "fitting_baseline": FIT_BASELINE,
                    "status": "reviewed",
                    "reviewer": reviewer.strip(),
                    "reviewed_at": reviewed_at.strip(),
                    "not_0624_or_migration_source": True,
                    "mature_contract": MATURE_CONTRACT,
                    "binder_schema": SCHEMA,
                    "queue_summary_path": next(
                        row["path"] for row in inventory if row["root_key"] == root_key and row["role"] == "queue_summary"
                    ),
                    "queue_summary_sha256": next(
                        row["sha256"] for row in inventory if row["root_key"] == root_key and row["role"] == "queue_summary"
                    ),
                    "queue_manifest_path": next(
                        row["path"] for row in inventory if row["root_key"] == root_key and row["role"] == "queue_manifest"
                    ),
                    "queue_manifest_sha256": next(
                        row["sha256"] for row in inventory if row["root_key"] == root_key and row["role"] == "queue_manifest"
                    ),
                    "evidence_inventory_sha256_pending_write": True,
                }
            )
    status = "pass" if root_results and len(families) == len(root_results) else "blocked"
    return {
        "schema": ATTESTATION_SCHEMA,
        "binder_schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": status,
        "root_count": len(root_results),
        "reviewed_family_count": len(families),
        "blocker_count": len(blockers),
        "families": families,
        "root_results": root_results,
        "blockers": blockers,
        "evidence_inventory": inventory,
        "source_paths": {
            "historical_replay_evidence_json": str(replay_path),
            "historical_replay_evidence_sha256": _sha256(replay_path),
            "algorithm_profile_path": str(profile_path),
            "algorithm_profile_sha256": _sha256(profile_path),
            "mature_route_contract_json": str(mature_path),
            "mature_route_contract_sha256": _sha256(mature_path),
            "automation_control_contract_json": str(automation_path),
            "automation_control_contract_sha256": _sha256(automation_path),
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(rows)


def write_v1_5_historical_route_attestation_binder(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "attestation_json": out / "v1_5_historical_route_baseline_attestation.json",
        "roots_csv": out / "v1_5_historical_route_attestation_roots.csv",
        "blockers_csv": out / "v1_5_historical_route_attestation_blockers.csv",
        "evidence_csv": out / "v1_5_historical_route_attestation_evidence_inventory.csv",
        "markdown": out / "V1_5_HISTORICAL_ROUTE_ATTESTATION_BINDER.md",
    }
    inventory = list(model.get("evidence_inventory") or [])
    _write_csv(outputs["roots_csv"], model.get("root_results") or [])
    _write_csv(outputs["blockers_csv"], model.get("blockers") or [])
    _write_csv(outputs["evidence_csv"], inventory)
    inventory_sha = _sha256(outputs["evidence_csv"])
    payload = dict(model)
    payload["families"] = []
    for row in model.get("families") or []:
        family = dict(row)
        family.pop("evidence_inventory_sha256_pending_write", None)
        family["evidence_inventory_path"] = str(outputs["evidence_csv"].resolve())
        family["evidence_inventory_sha256"] = inventory_sha
        payload["families"].append(family)
    payload["evidence_inventory_sha256"] = inventory_sha
    outputs["attestation_json"].write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# V1.5 Historical Route Attestation Binder",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- reviewed_family_count: `{model.get('reviewed_family_count')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        "- offline_only: `true`",
        "- not_real_acceptance_evidence: `true`",
        "",
        "A directory name is never sufficient. Reviewed families require an exact closed queue, mature point sidecars, samples, component QC, and bound file hashes.",
        "",
        "## Roots",
        "",
        "| Root | Profile | Status | Blockers |",
        "| --- | --- | --- | ---: |",
    ]
    for row in model.get("root_results") or []:
        lines.append(
            f"| `{row['root_key']}` | `{row['algorithm_profile_id']}` | `{row['status']}` | {row['blocker_count']} |"
        )
    lines.extend(["", "## Blocker Codes", ""])
    for code in sorted({row["code"] for row in model.get("blockers") or []}):
        lines.append(f"- `{code}`")
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "ATTESTATION_SCHEMA",
    "SCHEMA",
    "build_v1_5_historical_route_attestation_binder",
    "write_v1_5_historical_route_attestation_binder",
]
