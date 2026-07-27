"""Run V1.5 formal open-flow no-write CO2 sampling.

This sidecar runner is intentionally narrower than ``run_headless``. It keeps
PACE open to atmosphere, opens one requested CO2 source route, waits for purge
and analyzer stability, samples, then closes the route. It never enters sealed
pressure control and never writes analyzer coefficients or IDs. The default
analyzer path may issue MODE2/active-upload setup once, then samples by reading
the stream instead of repeatedly polling READDATA.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger
from ..utils.file_io import sha256_file
from ..validation.common import analyze_sample_rows
from ..validation.v1_5_component_qc_generator_contract import (
    FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS,
    FORMAL_EVIDENCE_BUNDLE_FILENAME,
    FORMAL_EVIDENCE_BUNDLE_SCHEMA,
    FORMAL_REFERENCE_SOURCE_RECORD_FILENAME,
    FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
)
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices


FORMAL_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S = 1.0
FORMAL_OPEN_FLOW_DEWPOINT_GATE_MAX_TOTAL_WAIT_S = 1800.0
FORMAL_OPEN_FLOW_ANALYZER_GATE_MAX_WAIT_S = 1800.0
FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S = 120.0


def _formal_sampling_completion_code(
    *,
    sampling_completed: bool,
    finalization_failures: Iterable[str],
) -> int:
    """Return success only when sampling and its evidence finalization both close."""

    return 0 if sampling_completed and not list(finalization_failures) else 1


def _formal_open_flow_dewpoint_gate_max_wait_s(value: Any, *, default: Optional[float] = None) -> float:
    """Normalize formal open-flow dewpoint wait without wasting standard gas.

    Temperature chamber settling has its own longer timeout. This cap applies
    only while a gas/water route is flowing and the program is waiting for
    dewpoint evidence to become physically acceptable.
    """

    fallback = FORMAL_OPEN_FLOW_DEWPOINT_GATE_MAX_TOTAL_WAIT_S if default is None else float(default)
    try:
        resolved = float(value if value is not None else fallback)
    except (TypeError, ValueError):
        resolved = fallback
    return min(max(0.0, resolved), FORMAL_OPEN_FLOW_DEWPOINT_GATE_MAX_TOTAL_WAIT_S)


def _formal_open_flow_analyzer_gate_max_wait_s(value: Any, *, default: Optional[float] = None) -> float:
    """Cap per-point analyzer ratio waits while a certified gas route is open."""

    fallback = FORMAL_OPEN_FLOW_ANALYZER_GATE_MAX_WAIT_S if default is None else float(default)
    try:
        resolved = float(value if value is not None else fallback)
    except (TypeError, ValueError):
        resolved = fallback
    return min(max(0.0, resolved), FORMAL_OPEN_FLOW_ANALYZER_GATE_MAX_WAIT_S)


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 no-write open-flow CO2 sampling without sealed pressure control."
    )
    parser.add_argument("--config", required=True, help="Runtime config JSON.")
    parser.add_argument("--run-id", default=None, help="Optional fixed run folder name.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--temp", type=float, default=20.0, help="Metadata temperature setpoint.")
    parser.add_argument("--co2-source-ppm", type=float, required=True, help="CO2 source valve nominal ppm.")
    parser.add_argument("--co2-group", default="B", help="CO2 source group, e.g. A or B.")
    parser.add_argument(
        "--certificate-co2-ppm",
        type=float,
        default=None,
        help="Certificate CO2 value stored as evidence metadata.",
    )
    parser.add_argument(
        "--certificate-uncertainty-ppm",
        type=float,
        default=None,
        help="Certificate uncertainty stored as evidence metadata.",
    )
    parser.add_argument(
        "--reference-source-catalog",
        default=None,
        help=(
            "Controlled reference-source catalog. Defaults to "
            "v1_5_reference_source_catalog.json beside the runtime config."
        ),
    )
    parser.add_argument(
        "--reference-asset-id",
        default=None,
        help=(
            "Explicit controlled certificate asset. When omitted, exactly one asset must match "
            "the certificate CO2 value and nominal source."
        ),
    )
    parser.add_argument("--pressure-target-hpa", type=float, default=None)
    parser.add_argument(
        "--n2-prepurge-s",
        type=float,
        default=0.0,
        help=(
            "Optional diagnostic nitrogen pre-purge duration through the same open-flow CO2 path. "
            "Default 0 disables it; this never writes coefficients or changes the formal CO2 source."
        ),
    )
    parser.add_argument(
        "--n2-purge-source-valve",
        type=int,
        default=None,
        help=(
            "Optional logical valve for the nitrogen source. When omitted the runner reads "
            "valves.nitrogen_purge_source / valves.n2_purge_source from the config."
        ),
    )
    parser.add_argument("--purge-s", type=float, default=360.0)
    parser.add_argument(
        "--minimum-purge-s",
        type=float,
        default=360.0,
        help=(
            "Formal minimum purge evidence for this point. The route may purge longer, "
            "but candidate-fit readiness must not treat shorter evidence as A grade."
        ),
    )
    parser.add_argument("--purge-trace-interval-s", type=float, default=10.0)
    parser.add_argument(
        "--max-open-flow-pressure-hpa",
        type=float,
        default=1100.0,
        help=(
            "Persistent open-flow pressure limit. Short startup spikes above this value are recorded as "
            "diagnostic evidence and only abort when they last beyond --open-flow-pressure-transient-grace-s."
        ),
    )
    parser.add_argument(
        "--open-flow-pressure-transient-grace-s",
        type=float,
        default=30.0,
        help="Allowed duration for startup/open-valve pressure spikes above --max-open-flow-pressure-hpa.",
    )
    parser.add_argument(
        "--open-flow-pressure-safety-hard-limit-hpa",
        type=float,
        default=1300.0,
        help="Immediate safety abort pressure during open-flow purge.",
    )
    parser.add_argument("--sample-count", type=int, default=10)
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--sensor-read-interval-s", type=float, default=5.0)
    parser.add_argument(
        "--analyzer-acquisition",
        choices=("active_stream_10hz", "active_stream_1hz", "passive_query"),
        default="active_stream_1hz",
        help=(
            "Gas-analyzer acquisition policy. The V1.5 formal default is "
            "active_stream_1hz, which sends FTD=01 and records one uploaded frame "
            "per formal sample anchor; active_stream_10hz reads the native 10 Hz "
            "stream without FTD; passive_query keeps the older READDATA fallback."
        ),
    )
    ftd_group = parser.add_mutually_exclusive_group()
    ftd_group.add_argument(
        "--allow-ftd-write",
        dest="allow_ftd_write",
        action="store_true",
        default=True,
        help="Allow the V1.5 formal 1 Hz active-upload setup command FTD=01. Never writes SENCO or ID.",
    )
    ftd_group.add_argument(
        "--no-ftd-write",
        dest="allow_ftd_write",
        action="store_false",
        help="Do not send FTD even if active_stream_1hz is selected; records expected 1 Hz only.",
    )
    parser.add_argument(
        "--min-valid-analyzers",
        type=int,
        default=1,
        help=(
            "Minimum analyzers that must show a stable CO2 ratio before formal sampling. "
            "Other analyzers remain in the evidence set and are classified by Frame QC."
        ),
    )
    parser.add_argument(
        "--analyzer-gate-required-labels",
        default="",
        help=(
            "Comma-separated analyzer labels that must pass the CO2 ratio gate before formal sampling. "
            "Use this for no-write recovery tests where only specific devices are eligible."
        ),
    )
    parser.add_argument(
        "--analyzer-gate-prefer-all-stable-grace-s",
        type=float,
        default=FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S,
        help=(
            "After the minimum analyzer count first becomes stable, keep the route open for this "
            "bounded grace period to let the remaining analyzers become A-grade before accepting "
            "min-valid evidence."
        ),
    )
    parser.add_argument(
        "--co2-ratio-f-preseal-tol",
        type=float,
        default=None,
        help="Optional explicit filtered CO2-ratio stability tolerance for the pre-sampling gate.",
    )
    parser.add_argument(
        "--co2-ratio-f-preseal-window-s",
        type=float,
        default=None,
        help="Optional explicit stable-window duration for the filtered CO2-ratio gate.",
    )
    parser.add_argument(
        "--co2-ratio-f-preseal-timeout-s",
        type=float,
        default=None,
        help="Optional explicit timeout for the filtered CO2-ratio gate.",
    )
    parser.add_argument(
        "--co2-ratio-f-preseal-min-samples",
        type=int,
        default=None,
        help="Optional explicit minimum sample count for the filtered CO2-ratio gate.",
    )
    parser.add_argument(
        "--co2-ratio-f-preseal-policy",
        choices=("reject", "warn", "pass"),
        default=None,
        help=(
            "CO2-ratio pre-sampling gate policy. The formal default is reject; warn continues "
            "after a failed gate only for diagnostic error quantification."
        ),
    )
    dewpoint_gate = parser.add_mutually_exclusive_group()
    dewpoint_gate.add_argument(
        "--gas-route-dewpoint-gate-enabled",
        dest="gas_route_dewpoint_gate_enabled",
        action="store_true",
        default=None,
        help="Require dry/stable route dewpoint before the formal CO2 sample window.",
    )
    dewpoint_gate.add_argument(
        "--no-gas-route-dewpoint-gate",
        dest="gas_route_dewpoint_gate_enabled",
        action="store_false",
        help="Disable the route dewpoint gate for engineering diagnostics only.",
    )
    parser.add_argument("--gas-route-dewpoint-gate-policy", choices=("reject", "warn", "pass"), default=None)
    dry_gate = parser.add_mutually_exclusive_group()
    dry_gate.add_argument(
        "--gas-route-dewpoint-require-dry-enough",
        dest="gas_route_dewpoint_require_dry_enough",
        action="store_true",
        default=None,
    )
    dry_gate.add_argument(
        "--no-gas-route-dewpoint-require-dry-enough",
        dest="gas_route_dewpoint_require_dry_enough",
        action="store_false",
    )
    parser.add_argument("--gas-route-dewpoint-dry-enough-c", type=float, default=None)
    parser.add_argument("--gas-route-dewpoint-gate-max-total-wait-s", type=float, default=None)
    parser.add_argument("--gas-route-dewpoint-gate-window-s", type=float, default=None)
    parser.add_argument("--gas-route-dewpoint-gate-tail-span-max-c", type=float, default=None)
    parser.add_argument("--gas-route-dewpoint-gate-tail-slope-abs-max-c-per-s", type=float, default=None)
    parser.add_argument("--gas-route-dewpoint-gate-deep-dry-tail-relax-margin-c", type=float, default=None)
    parser.add_argument("--skip-stability-gate", action="store_true")
    parser.add_argument("--no-prompt", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _resolve_reference_document_path(
    catalog_path: Path,
    document: Dict[str, Any],
) -> Path:
    kind = str(document.get("source_path_kind") or "").strip()
    relative_text = str(document.get("relative_path") or "").strip()
    relative_path = Path(relative_text)
    if not relative_text or relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("reference_document_relative_path_invalid")
    if kind == "user_profile_relative":
        root = Path.home().resolve()
    elif kind == "catalog_relative":
        root = catalog_path.resolve().parent
    else:
        raise ValueError(f"reference_document_path_kind_unsupported:{kind}")
    resolved = (root / relative_path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("reference_document_path_outside_allowed_root") from exc
    return resolved


def _build_co2_reference_source_record(
    catalog_path: Path,
    *,
    run_id: str,
    co2_source_ppm: float,
    certificate_co2_ppm: Optional[float],
    reference_asset_id: Optional[str] = None,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """Validate one CO2 source before any device can be constructed."""

    checked_on = today or datetime.now().date()
    record: Dict[str, Any] = {
        "schema_version": FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
        "run_id": str(run_id),
        "route_kind": "co2",
        "reference_source_status": "fail",
        "selection_mode": "explicit_asset_id" if reference_asset_id else "unique_value_and_nominal_match",
        "catalog_path": str(catalog_path.resolve()),
        "catalog_sha256": None,
        "requested_reference_asset_id": str(reference_asset_id or ""),
        "input": {
            "co2_source_ppm": float(co2_source_ppm),
            "certificate_co2_ppm": (
                None if certificate_co2_ppm is None else float(certificate_co2_ppm)
            ),
        },
        "selected_asset": None,
        "documents_verified": [],
        "checked_on": checked_on.isoformat(),
        "physical_reference_contract": (
            "CO2 formal target uses the selected controlled asset value. Dry-air zero remains a "
            "separate operator-confirmed previous-calibration anchor and is not represented as a "
            "CO2 value printed on the dry-air certificate."
        ),
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "reasons": [],
    }
    reasons: List[str] = record["reasons"]
    try:
        if not catalog_path.is_file():
            raise ValueError("reference_source_catalog_missing")
        record["catalog_sha256"] = sha256_file(catalog_path)
        with catalog_path.open("r", encoding="utf-8-sig") as handle:
            catalog = json.load(handle)
        if not isinstance(catalog, dict):
            raise ValueError("reference_source_catalog_not_object")
        if catalog.get("schema_version") != "v1_5_reference_source_catalog_v1":
            raise ValueError("reference_source_catalog_schema_invalid")
        if catalog.get("not_real_acceptance_evidence") is not True:
            raise ValueError("reference_source_catalog_real_acceptance_lock_missing")
        assets = [
            row
            for row in catalog.get("assets", [])
            if isinstance(row, dict) and str(row.get("route_kind") or "").lower() == "co2"
        ]
        if reference_asset_id:
            matches = [
                row for row in assets if str(row.get("asset_id") or "") == str(reference_asset_id)
            ]
        elif certificate_co2_ppm is None:
            matches = []
            reasons.append("certificate_co2_ppm_required_for_automatic_selection")
        else:
            matches = [
                row
                for row in assets
                if abs(float(row.get("certificate_co2_ppm")) - float(certificate_co2_ppm)) <= 0.005
                and abs(float(row.get("nominal_co2_ppm")) - float(co2_source_ppm)) <= 1.0
            ]
        if len(matches) != 1:
            reasons.append(f"reference_asset_match_count:{len(matches)}")
            return record
        asset = copy.deepcopy(matches[0])
        record["selected_asset"] = asset
        if not str(asset.get("asset_id") or "").strip():
            reasons.append("reference_asset_id_missing")
        if not str(asset.get("cylinder_number") or "").strip():
            reasons.append("reference_asset_cylinder_number_missing")
        if str(asset.get("document_kind") or "") not in {
            "full_certificate_photo_pair",
            "cylinder_certificate_label_photo_pair",
        }:
            reasons.append("reference_asset_document_kind_invalid")
        if abs(float(asset.get("nominal_co2_ppm")) - float(co2_source_ppm)) > 1.0:
            reasons.append("reference_asset_nominal_source_mismatch")
        if certificate_co2_ppm is None:
            reasons.append("certificate_co2_ppm_missing")
        elif abs(float(asset.get("certificate_co2_ppm")) - float(certificate_co2_ppm)) > 0.005:
            reasons.append("reference_asset_certificate_value_mismatch")
        if asset.get("documentary_use_status") != "operator_authorized_for_v1_5_evidence":
            reasons.append("reference_asset_not_operator_authorized")
        if asset.get("calibration_fit_reference_allowed") is not True:
            reasons.append("reference_asset_not_allowed_for_fit_reference")
        if float(asset.get("nominal_co2_ppm")) == 0.0:
            if asset.get("physical_role") != "co2_zero_gas":
                reasons.append("dry_air_zero_physical_role_invalid")
            if asset.get("reference_value_source") != "operator_confirmed_previous_calibration":
                reasons.append("dry_air_zero_value_source_invalid")
            if asset.get("co2_value_directly_certified") is not False:
                reasons.append("dry_air_zero_must_not_claim_direct_co2_certification")
        elif asset.get("co2_value_directly_certified") is not True:
            reasons.append("standard_gas_direct_co2_certification_missing")
        elif asset.get("physical_role") != "co2_standard_gas":
            reasons.append("standard_gas_physical_role_invalid")
        try:
            issue_date = date.fromisoformat(str(asset.get("issue_date") or ""))
            valid_through = date.fromisoformat(str(asset.get("valid_through") or ""))
        except ValueError:
            reasons.append("reference_asset_validity_date_invalid")
        else:
            if valid_through < issue_date:
                reasons.append("reference_asset_validity_range_invalid")
            if checked_on < issue_date:
                reasons.append("reference_asset_not_yet_valid")
            if checked_on > valid_through:
                reasons.append("reference_asset_expired")
        documents = asset.get("documents")
        if not isinstance(documents, list) or not documents:
            reasons.append("reference_asset_documents_missing")
        else:
            verified: List[Dict[str, Any]] = []
            for index, document in enumerate(documents):
                if not isinstance(document, dict):
                    reasons.append(f"reference_document_invalid:{index}")
                    continue
                try:
                    source_path = _resolve_reference_document_path(catalog_path, document)
                except ValueError as exc:
                    reasons.append(f"{exc}:{index}")
                    continue
                expected_size = int(document.get("size_bytes") or -1)
                expected_sha = str(document.get("sha256") or "").lower()
                row = {
                    "page_role": str(document.get("page_role") or ""),
                    "source_path_kind": str(document.get("source_path_kind") or ""),
                    "relative_path": str(document.get("relative_path") or ""),
                    "expected_size_bytes": expected_size,
                    "expected_sha256": expected_sha,
                    "observed_size_bytes": None,
                    "observed_sha256": None,
                    "verified": False,
                }
                if not source_path.is_file():
                    reasons.append(f"reference_document_missing:{index}")
                else:
                    row["observed_size_bytes"] = source_path.stat().st_size
                    row["observed_sha256"] = sha256_file(source_path)
                    row["verified"] = (
                        row["observed_size_bytes"] == expected_size
                        and row["observed_sha256"] == expected_sha
                    )
                    if row["observed_size_bytes"] != expected_size:
                        reasons.append(f"reference_document_size_mismatch:{index}")
                    if row["observed_sha256"] != expected_sha:
                        reasons.append(f"reference_document_sha256_mismatch:{index}")
                verified.append(row)
            record["documents_verified"] = verified
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        reasons.append(str(exc))
    record["reasons"] = sorted(set(str(reason) for reason in reasons if reason))
    record["reference_source_status"] = "pass" if not record["reasons"] else "fail"
    return record


def _write_formal_reference_source_record(
    run_dir: Path,
    payload: Dict[str, Any],
) -> Path:
    path = run_dir / FORMAL_REFERENCE_SOURCE_RECORD_FILENAME
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _configured_analyzer_labels(cfg: Dict[str, Any]) -> List[str]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg.get("devices"), dict) else {}
    labels: List[str] = []
    analyzers = devices_cfg.get("gas_analyzers")
    if isinstance(analyzers, list) and analyzers:
        for idx, analyzer_cfg in enumerate(analyzers, start=1):
            if isinstance(analyzer_cfg, dict):
                raw = analyzer_cfg.get("name") or analyzer_cfg.get("label")
            else:
                raw = None
            labels.append(str(raw or f"ga{idx:02d}").strip() or f"ga{idx:02d}")
        return labels
    if isinstance(devices_cfg.get("gas_analyzer"), dict):
        raw = devices_cfg["gas_analyzer"].get("name") or devices_cfg["gas_analyzer"].get("label")
        return [str(raw or "ga01").strip() or "ga01"]
    return []


def _parse_label_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value or "").split(",")
    labels: List[str] = []
    for item in raw_items:
        label = str(item or "").strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def _normalize_analyzer_acquisition_policy(value: str) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "active": "active_stream_10hz",
        "active_stream": "active_stream_10hz",
        "active_upload": "active_stream_10hz",
        "active_upload_10hz": "active_stream_10hz",
        "active_stream_10hz": "active_stream_10hz",
        "active_1hz": "active_stream_1hz",
        "active_upload_1hz": "active_stream_1hz",
        "active_stream_1hz": "active_stream_1hz",
        "passive": "passive_query",
        "passive_query": "passive_query",
        "passive_poll": "passive_query",
        "passive_query_per_device": "passive_query",
    }
    return aliases.get(text, "active_stream_10hz")


def _apply_analyzer_acquisition_policy(
    runtime_cfg: Dict[str, Any],
    *,
    analyzer_acquisition: str,
    sensor_read_interval_s: float,
    sample_interval_s: float,
    allow_ftd_write: bool = False,
) -> str:
    policy = _normalize_analyzer_acquisition_policy(analyzer_acquisition)
    active_stream = policy in {"active_stream_10hz", "active_stream_1hz"}
    target_hz = 1 if policy == "active_stream_1hz" else 10

    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in ("gas_analyzer",):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["active_send"] = bool(active_stream)
            if active_stream:
                devices_cfg[key]["ftd_hz"] = target_hz
    if isinstance(devices_cfg.get("gas_analyzers"), list):
        for analyzer_cfg in devices_cfg["gas_analyzers"]:
            if isinstance(analyzer_cfg, dict):
                analyzer_cfg["active_send"] = bool(active_stream)
                if active_stream:
                    analyzer_cfg["ftd_hz"] = target_hz

    metadata = runtime_cfg.setdefault("metadata", {})
    if policy == "active_stream_1hz":
        metadata["analyzer_acquisition_policy"] = (
            "active_mode2_stream_1hz_ftd01_controlled"
            if allow_ftd_write
            else "active_mode2_stream_existing_rate_no_ftd_requested_1hz"
        )
    else:
        metadata["analyzer_acquisition_policy"] = (
            "active_mode2_stream_10hz_no_ftd" if active_stream else "passive_query_per_device"
        )
    metadata["analyzer_stream_target_hz"] = float(target_hz) if active_stream else None
    metadata["analyzer_stream_native_hz"] = (
        float(target_hz)
        if active_stream and (policy != "active_stream_1hz" or allow_ftd_write)
        else None
    )
    metadata["analyzer_stream_frequency_control"] = (
        "FTD01_written"
        if policy == "active_stream_1hz" and allow_ftd_write
        else "existing_device_setting_no_ftd_write"
        if active_stream
        else "passive_query"
    )
    metadata["ftd_write_enabled"] = bool(policy == "active_stream_1hz" and allow_ftd_write)
    metadata["formal_sample_anchor_interval_s"] = float(sample_interval_s)
    metadata["formal_sample_decimation"] = (
        "nearest_usable_mode2_frame_at_1hz_anchor_from_1hz_stream"
        if policy == "active_stream_1hz" and allow_ftd_write
        else "nearest_usable_mode2_frame_at_1hz_anchor_from_existing_stream_no_ftd"
        if policy == "active_stream_1hz"
        else "nearest_usable_mode2_frame_at_1hz_anchor_from_10hz_stream"
        if policy == "active_stream_10hz"
        else "passive_query_cache_at_formal_sample_anchor"
    )
    metadata["startup_mode2_missing_policy"] = (
        "mode2_stream_config_then_sampling_qc" if active_stream else "defer_to_sampling_qc"
    )

    live_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("analyzer_live_snapshot", {})
    live_cfg["enabled"] = bool(active_stream)
    live_cfg["sampling_worker_enabled"] = True
    live_cfg["passive_round_robin_enabled"] = not active_stream
    live_cfg["passive_per_device_workers_enabled"] = not active_stream
    if active_stream:
        worker_interval = 0.2 if policy == "active_stream_1hz" else 0.1
        live_cfg["sampling_worker_interval_s"] = min(
            worker_interval,
            float(live_cfg.get("sampling_worker_interval_s", worker_interval) or worker_interval),
        )
        live_cfg["active_drain_poll_s"] = min(
            0.08,
            float(live_cfg.get("active_drain_poll_s", 0.08) or 0.08),
        )
        live_cfg["active_ring_buffer_size"] = max(
            512 if policy == "active_stream_10hz" else 128,
            int(live_cfg.get("active_ring_buffer_size", 0) or 0),
        )
        live_cfg["active_frame_max_anchor_delta_ms"] = max(
            800.0 if policy == "active_stream_1hz" else 450.0,
            float(live_cfg.get("active_frame_max_anchor_delta_ms", 0.0) or 0.0),
        )
        live_cfg["active_frame_right_match_max_ms"] = max(
            400.0 if policy == "active_stream_1hz" else 180.0,
            float(live_cfg.get("active_frame_right_match_max_ms", 0.0) or 0.0),
        )
        live_cfg["active_frame_stale_ms"] = max(
            2500.0 if policy == "active_stream_1hz" else 1200.0,
            float(live_cfg.get("active_frame_stale_ms", 0.0) or 0.0),
        )
    else:
        live_cfg["passive_round_robin_interval_s"] = max(0.5, float(sensor_read_interval_s))
        live_cfg["cache_ttl_s"] = max(
            float(live_cfg.get("cache_ttl_s", 0.0) or 0.0),
            float(sensor_read_interval_s) * 1.5,
            float(sample_interval_s) * 2.0,
        )

    sampling_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})
    sampling_cfg["pre_sample_analyzer_max_age_s"] = max(
        float(sampling_cfg.get("pre_sample_analyzer_max_age_s", 0.0) or 0.0),
        2.5
        if policy == "active_stream_1hz"
        else 1.2
        if active_stream
        else max(1.5, float(sensor_read_interval_s) * 1.5),
    )
    sensor_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("stability", {}).setdefault("sensor", {})
    sensor_cfg.setdefault("co2_ratio_f_preseal_flush_active_stream_before_gate", True)

    init_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("analyzer_mode2_init", {})
    if policy == "active_stream_1hz":
        init_cfg["send_active_freq"] = bool(allow_ftd_write)
        init_cfg["reapply_attempts"] = max(int(init_cfg.get("reapply_attempts", 1) or 1), 2)
        init_cfg["stream_attempts"] = max(int(init_cfg.get("stream_attempts", 10) or 10), 15)
        init_cfg["ready_consecutive_frames"] = max(
            int(init_cfg.get("ready_consecutive_frames", 2) or 2),
            2,
        )
        init_cfg["retry_delay_s"] = max(float(init_cfg.get("retry_delay_s", 0.2) or 0.2), 0.25)
        init_cfg["reapply_delay_s"] = max(float(init_cfg.get("reapply_delay_s", 0.35) or 0.35), 1.0)
        init_cfg["command_gap_s"] = max(float(init_cfg.get("command_gap_s", 1.0) or 1.0), 1.0)
        init_cfg["post_enable_stream_wait_s"] = max(
            float(init_cfg.get("post_enable_stream_wait_s", 2.0) or 2.0),
            4.0,
        )
        init_cfg["post_enable_stream_ack_wait_s"] = max(
            float(init_cfg.get("post_enable_stream_ack_wait_s", 8.0) or 8.0),
            10.0,
        )
    elif active_stream:
        init_cfg["send_active_freq"] = False
    if active_stream:
        init_cfg["write_config_on_read_first_fail"] = True
        init_cfg["skip_config_when_read_first_ready"] = True
    return policy


def _enable_formal_summary_outlier_filter(runtime_cfg: Dict[str, Any]) -> None:
    sampling_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})
    filter_cfg = sampling_cfg.setdefault("summary_outlier_filter", {})
    try:
        min_samples = int(float(filter_cfg.get("min_samples", 5) or 5))
    except Exception:
        min_samples = 5
    try:
        max_outliers = int(float(filter_cfg.get("max_outliers_per_key", 1) or 1))
    except Exception:
        max_outliers = 1
    filter_cfg["enabled"] = True
    filter_cfg["scope"] = "per_analyzer_sample_window_summary_only"
    filter_cfg["raw_frame_retention"] = "all_raw_frames_kept"
    filter_cfg["keys"] = ["co2_ratio_f", "h2o_ratio_f"]
    filter_cfg["min_samples"] = max(min_samples, 5)
    filter_cfg["max_outliers_per_key"] = min(max(max_outliers, 1), 1)
    thresholds = filter_cfg.setdefault("absolute_thresholds", {})
    if isinstance(thresholds, dict):
        thresholds.setdefault("co2_ratio_f", 0.001)
        thresholds.setdefault("h2o_ratio_f", 0.001)


V1_5_TEMPERATURE_TRUTH_SOURCE = "in_chamber_platinum_resistance_digital_thermometer"


def _apply_v1_5_temperature_truth_contract(
    runtime_cfg: Dict[str, Any],
    *,
    require_device_config: bool = False,
) -> Dict[str, Any]:
    devices_cfg = runtime_cfg.setdefault("devices", {})
    thermometer_cfg = devices_cfg.get("thermometer")
    if not isinstance(thermometer_cfg, dict):
        if require_device_config:
            raise RuntimeError("thermometer config is missing for V1.5 temperature truth")
    else:
        thermometer_cfg["enabled"] = True

    temp_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("stability", {}).setdefault(
        "temperature",
        {},
    )
    temp_cfg["temperature_truth_source"] = V1_5_TEMPERATURE_TRUTH_SOURCE
    temp_cfg["thermometer_truth_required"] = True
    temp_cfg["temperature_chamber_setpoint_substitution_forbidden"] = True
    return temp_cfg


def _verify_v1_5_point_temperature_truth(
    runtime_cfg: Dict[str, Any],
    devices: Dict[str, Any],
    *,
    target_c: float,
    log: Any = _log,
) -> bool:
    temp_cfg = runtime_cfg.get("workflow", {}).get("stability", {}).get("temperature", {})
    required = bool(
        temp_cfg.get("thermometer_truth_required", False)
        or temp_cfg.get("temperature_chamber_setpoint_substitution_forbidden", False)
    )
    if not required:
        return True

    thermometer = devices.get("thermometer")
    chamber = devices.get("temp_chamber")
    if not callable(getattr(thermometer, "read_temp_c", None)) or not callable(
        getattr(chamber, "read_temp_c", None)
    ):
        log("V1.5 point temperature truth gate failed: thermometer or chamber is unavailable")
        return False
    try:
        thermometer_c = float(thermometer.read_temp_c())
        chamber_c = float(chamber.read_temp_c())
    except (TypeError, ValueError, OSError, RuntimeError) as exc:
        log(f"V1.5 point temperature truth gate failed: {exc}")
        return False

    tol_c = abs(float(temp_cfg.get("thermometer_truth_tol_c", temp_cfg.get("tol", 0.2)) or 0.2))
    chamber_tol_c = abs(float(temp_cfg.get("chamber_control_tol_c", temp_cfg.get("tol", 0.2)) or 0.2))
    command_offset_c = float(temp_cfg.get("command_offset_c", 0.0) or 0.0)
    command_target_c = float(target_c) + command_offset_c
    ok = (
        abs(thermometer_c - float(target_c)) <= tol_c
        and abs(chamber_c - command_target_c) <= chamber_tol_c
    )
    log(
        "V1.5 point temperature truth gate: "
        f"ok={ok}, target={float(target_c):.2f}, thermometer={thermometer_c:.2f}, "
        f"chamber={chamber_c:.2f}, command_target={command_target_c:.2f}"
    )
    return ok


def _prepare_runtime_cfg(
    cfg: Dict[str, Any],
    *,
    output_dir: Optional[str],
    sample_count: int,
    sample_interval_s: float,
    sensor_read_interval_s: float,
    min_valid_analyzers: int = 1,
    analyzer_acquisition: str = "active_stream_1hz",
    allow_ftd_write: bool = True,
    analyzer_gate_required_labels: Optional[Iterable[str]] = None,
    analyzer_gate_prefer_all_stable_grace_s: Optional[float] = None,
    co2_ratio_f_preseal_tol: Optional[float] = None,
    co2_ratio_f_preseal_window_s: Optional[float] = None,
    co2_ratio_f_preseal_timeout_s: Optional[float] = None,
    co2_ratio_f_preseal_min_samples: Optional[int] = None,
    co2_ratio_f_preseal_policy: Optional[str] = None,
    gas_route_dewpoint_gate_enabled: Optional[bool] = None,
    gas_route_dewpoint_gate_policy: Optional[str] = None,
    gas_route_dewpoint_require_dry_enough: Optional[bool] = None,
    gas_route_dewpoint_dry_enough_c: Optional[float] = None,
    gas_route_dewpoint_gate_max_total_wait_s: Optional[float] = None,
    gas_route_dewpoint_gate_window_s: Optional[float] = None,
    gas_route_dewpoint_gate_tail_span_max_c: Optional[float] = None,
    gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s: Optional[float] = None,
    gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c: Optional[float] = None,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    runtime_cfg["workflow"]["skip_h2o"] = True
    runtime_cfg["workflow"]["route_mode"] = "co2_open_flow_sidecar"
    runtime_cfg.setdefault("metadata", {})["formal_open_flow_sidecar"] = True
    runtime_cfg["metadata"]["writes_senco"] = False
    runtime_cfg["metadata"]["writes_device_id"] = False
    runtime_cfg["metadata"]["sealed_pressure_points_enter_formal_fit"] = False
    runtime_cfg["metadata"]["open_flow_sampling_physical_contract"] = {
        "sample_window_requires_route_open": True,
        "sample_window_requires_standard_gas_open_flow": True,
        "route_close_allowed_only_after_sample_window": True,
        "dryness_or_dewpoint_gate_required": bool(
            gas_route_dewpoint_gate_enabled
            if gas_route_dewpoint_gate_enabled is not None
            else runtime_cfg.get("workflow", {})
            .get("stability", {})
            .get("gas_route_dewpoint_gate_enabled", False)
        ),
        "per_analyzer_ratio_stability_required": True,
        "per_analyzer_status_register_qc_required": True,
        "unstable_analyzer_handling": "prefer_all_stable_with_bounded_grace_then_independent_grade_or_reject",
        "pressure_role": "traceability_and_qc_input_not_co2_fit_variable",
        "sample_readiness_basis": "dry_enough_dewpoint_plus_ratio_stability_plus_best_live_stable_window",
        "normal_point_timeout_s": FORMAL_OPEN_FLOW_ANALYZER_GATE_MAX_WAIT_S,
        "extreme_display_values_require_factory_signal_root_cause_review": True,
    }
    _apply_v1_5_temperature_truth_contract(runtime_cfg)

    devices_cfg = runtime_cfg.setdefault("devices", {})
    if isinstance(devices_cfg.get("humidity_generator"), dict):
        devices_cfg["humidity_generator"]["enabled"] = False

    runtime_cfg["metadata"]["ftd_write_enabled"] = False
    runtime_cfg["metadata"]["idle_continuous_atmosphere_hold"] = False
    runtime_cfg["metadata"]["continuous_atmosphere_hold_scope"] = "open_flow_purge_and_sampling_only"
    runtime_cfg["metadata"]["startup_mode2_missing_policy"] = "mode2_stream_config_then_sampling_qc"

    pressure_cfg = runtime_cfg["workflow"].setdefault("pressure", {})
    pressure_cfg["continuous_atmosphere_hold"] = False

    init_cfg = runtime_cfg["workflow"].setdefault("analyzer_mode2_init", {})
    init_cfg["read_first_before_config"] = True
    init_cfg["sniff_stream_before_config"] = True
    init_cfg["write_config_on_read_first_fail"] = False
    init_cfg["send_active_freq"] = False

    sampling_cfg = runtime_cfg["workflow"].setdefault("sampling", {})
    sampling_cfg["count"] = int(sample_count)
    sampling_cfg["stable_count"] = int(sample_count)
    sampling_cfg["interval_s"] = float(sample_interval_s)
    sampling_cfg["co2_interval_s"] = float(sample_interval_s)
    sampling_cfg["pre_sample_freshness_timeout_s"] = max(
        5.0,
        float(sampling_cfg.get("pre_sample_freshness_timeout_s", 0.0) or 0.0),
    )
    sampling_cfg["pre_sample_signal_max_age_s"] = max(
        1.5,
        float(sampling_cfg.get("pre_sample_signal_max_age_s", 0.0) or 0.0),
    )
    _enable_formal_summary_outlier_filter(runtime_cfg)
    _apply_analyzer_acquisition_policy(
        runtime_cfg,
        analyzer_acquisition=analyzer_acquisition,
        sensor_read_interval_s=sensor_read_interval_s,
        sample_interval_s=sample_interval_s,
        allow_ftd_write=allow_ftd_write,
    )

    sensor_cfg = runtime_cfg["workflow"].setdefault("stability", {}).setdefault("sensor", {})
    sensor_cfg["read_interval_s"] = max(0.2, float(sensor_read_interval_s))
    sensor_cfg["co2_ratio_f_preseal_read_interval_s"] = max(0.2, float(sensor_read_interval_s))
    if co2_ratio_f_preseal_tol is not None:
        sensor_cfg["co2_ratio_f_preseal_tol"] = float(co2_ratio_f_preseal_tol)
    else:
        base_tol = float(sensor_cfg.get("co2_ratio_f_tol", 0.001) or 0.001)
        current_tol = float(sensor_cfg.get("co2_ratio_f_preseal_tol", base_tol) or base_tol)
        sensor_cfg["co2_ratio_f_preseal_tol"] = min(current_tol, base_tol)
    co2_hard_tol = float(sensor_cfg.get("co2_ratio_f_preseal_tol", 0.001) or 0.001)
    co2_a_grade_tol = float(
        sensor_cfg.get("co2_ratio_f_preseal_a_grade_tol", min(0.0005, co2_hard_tol))
        or min(0.0005, co2_hard_tol)
    )
    sensor_cfg["co2_ratio_f_preseal_a_grade_tol"] = min(co2_a_grade_tol, co2_hard_tol)
    if co2_ratio_f_preseal_window_s is not None:
        sensor_cfg["co2_ratio_f_preseal_window_s"] = float(co2_ratio_f_preseal_window_s)
    if co2_ratio_f_preseal_timeout_s is not None:
        sensor_cfg["co2_ratio_f_preseal_timeout_s"] = _formal_open_flow_analyzer_gate_max_wait_s(
            co2_ratio_f_preseal_timeout_s
        )
    else:
        sensor_cfg["co2_ratio_f_preseal_timeout_s"] = _formal_open_flow_analyzer_gate_max_wait_s(
            sensor_cfg.get("co2_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300)),
            default=300.0,
        )
    if co2_ratio_f_preseal_min_samples is not None:
        sensor_cfg["co2_ratio_f_preseal_min_samples"] = int(co2_ratio_f_preseal_min_samples)
    if co2_ratio_f_preseal_policy is not None:
        sensor_cfg["co2_ratio_f_preseal_policy"] = str(co2_ratio_f_preseal_policy).strip().lower()
    stability_cfg = runtime_cfg["workflow"].setdefault("stability", {})
    if gas_route_dewpoint_gate_enabled is not None:
        stability_cfg["gas_route_dewpoint_gate_enabled"] = bool(gas_route_dewpoint_gate_enabled)
    if gas_route_dewpoint_gate_policy is not None:
        stability_cfg["gas_route_dewpoint_gate_policy"] = str(gas_route_dewpoint_gate_policy).strip().lower()
    if gas_route_dewpoint_require_dry_enough is not None:
        stability_cfg["gas_route_dewpoint_gate_require_dry_enough"] = bool(
            gas_route_dewpoint_require_dry_enough
        )
    elif stability_cfg.get("gas_route_dewpoint_gate_enabled", True):
        stability_cfg["gas_route_dewpoint_gate_require_dry_enough"] = bool(
            stability_cfg.get("gas_route_dewpoint_gate_require_dry_enough", True)
        )
    if gas_route_dewpoint_dry_enough_c is not None:
        stability_cfg["gas_route_dewpoint_gate_dry_enough_c"] = float(gas_route_dewpoint_dry_enough_c)
    if gas_route_dewpoint_gate_max_total_wait_s is not None:
        stability_cfg["gas_route_dewpoint_gate_max_total_wait_s"] = (
            _formal_open_flow_dewpoint_gate_max_wait_s(gas_route_dewpoint_gate_max_total_wait_s)
        )
    else:
        stability_cfg["gas_route_dewpoint_gate_max_total_wait_s"] = (
            _formal_open_flow_dewpoint_gate_max_wait_s(
                stability_cfg.get("gas_route_dewpoint_gate_max_total_wait_s")
            )
        )
    if gas_route_dewpoint_gate_window_s is not None:
        stability_cfg["gas_route_dewpoint_gate_window_s"] = float(gas_route_dewpoint_gate_window_s)
    if gas_route_dewpoint_gate_tail_span_max_c is not None:
        stability_cfg["gas_route_dewpoint_gate_tail_span_max_c"] = float(
            gas_route_dewpoint_gate_tail_span_max_c
        )
    if gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s is not None:
        stability_cfg["gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s"] = float(
            gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s
        )
    if gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c is not None:
        stability_cfg["gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c"] = float(
            gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c
        )
    elif stability_cfg.get("gas_route_dewpoint_gate_enabled", True):
        stability_cfg["gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c"] = float(
            stability_cfg.get("gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c", 4.0) or 0.0
        )
    analyzer_labels = _configured_analyzer_labels(runtime_cfg)
    required_labels = _parse_label_list(analyzer_gate_required_labels)
    if analyzer_labels:
        required_labels = [label for label in required_labels if label in analyzer_labels]
    optional_labels = [label for label in analyzer_labels if label not in set(required_labels)]
    min_valid = max(1, int(min_valid_analyzers))
    if required_labels:
        min_valid = max(min_valid, len(required_labels))
    if analyzer_labels:
        min_valid = min(min_valid, len(analyzer_labels))
    stability_cfg["analyzer_gate_min_valid_analyzers"] = min_valid
    stability_cfg["analyzer_gate_optional_labels"] = optional_labels if required_labels else analyzer_labels
    stability_cfg["analyzer_gate_required_labels"] = required_labels
    stability_cfg["analyzer_gate_allow_pass_with_dropped_optional"] = True
    stability_cfg["analyzer_gate_disable_dropped_optional"] = False
    stability_cfg["analyzer_gate_zero_value_policy"] = "drop_optional_not_block"
    stability_cfg["analyzer_gate_invalid_frame_min_count"] = 3
    stability_cfg["analyzer_gate_silent_timeout_s"] = max(15.0, float(sensor_read_interval_s) * 3.0)
    stability_cfg["analyzer_gate_max_wait_s"] = _formal_open_flow_analyzer_gate_max_wait_s(
        max(
            float(sensor_cfg.get("co2_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300)) or 300),
            90.0,
        ),
        default=300.0,
    )
    prefer_all_grace_s = (
        FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S
        if analyzer_gate_prefer_all_stable_grace_s is None
        else float(analyzer_gate_prefer_all_stable_grace_s)
    )
    stability_cfg["analyzer_gate_prefer_all_stable_grace_s"] = min(
        max(0.0, prefer_all_grace_s),
        float(stability_cfg["analyzer_gate_max_wait_s"]),
    )
    temp_cfg = runtime_cfg["workflow"].setdefault("stability", {}).setdefault("temperature", {})
    current_span = float(temp_cfg.get("analyzer_chamber_temp_span_c", 0.08) or 0.08)
    temp_cfg["analyzer_chamber_temp_span_c"] = max(current_span, 0.08)
    current_window = float(temp_cfg.get("analyzer_chamber_temp_window_s", 60.0) or 60.0)
    temp_cfg["analyzer_chamber_temp_window_s"] = max(current_window, 60.0)

    postrun_cfg = runtime_cfg["workflow"].setdefault("postrun_corrected_delivery", {})
    postrun_cfg["enabled"] = False
    postrun_cfg["write_devices"] = False
    postrun_cfg["write_pressure_coefficients"] = False
    startup_pressure_cfg = runtime_cfg["workflow"].setdefault("startup_pressure_sensor_calibration", {})
    startup_pressure_cfg["enabled"] = False
    startup_pressure_cfg["apply_write"] = False

    if output_dir:
        runtime_cfg.setdefault("paths", {})["output_dir"] = str(Path(output_dir).resolve())
    return runtime_cfg


def _defer_startup_mode2_disabled_analyzers(runner: CalibrationRunner) -> List[str]:
    """Keep fragile analyzers in the run when startup read-first sees no frame.

    Some field analyzers intermittently miss the initial read-first probe but
    answer the later passive per-device query. For these V1.5 formal sidecars
    we avoid startup configuration writes, so final eligibility belongs to the
    sampling Frame QC rather than to the startup probe alone.
    """

    reasons = getattr(runner, "_disabled_analyzer_reasons", {})
    labels = [
        str(label)
        for label, reason in list(reasons.items())
        if str(reason) == "startup_mode2_verify_failed"
    ]
    if not labels:
        return []
    disabled = getattr(runner, "_disabled_analyzers", set())
    last_reprobe = getattr(runner, "_disabled_analyzer_last_reprobe_ts", {})
    for label in labels:
        try:
            disabled.discard(label)
        except Exception:
            pass
        try:
            reasons.pop(label, None)
        except Exception:
            pass
        try:
            last_reprobe.pop(label, None)
        except Exception:
            pass
    log_event = getattr(runner, "_log_run_event", None)
    if callable(log_event):
        log_event(
            command="analyzer-startup-mode2-deferred-to-sampling-qc",
            response=json.dumps({"labels": labels}, ensure_ascii=False),
        )
    runner.log(
        "Analyzer startup MODE2 proof deferred to sampling Frame QC: "
        + ", ".join(labels)
    )
    return labels


def _build_open_flow_point(
    *,
    temp_c: float,
    co2_source_ppm: float,
    co2_group: str,
    pressure_target_hpa: Optional[float],
) -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=float(temp_c),
        co2_ppm=float(co2_source_ppm),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_target_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=str(co2_group or "").strip().upper() or None,
    )


def _apply_certificate_target_after_valve_selection(
    point: CalibrationPoint,
    certificate_co2_ppm: Optional[float],
) -> None:
    """Use certificate concentration for calibration math after valve selection."""

    if certificate_co2_ppm is not None:
        point.co2_ppm = float(certificate_co2_ppm)


def _as_optional_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _nitrogen_purge_source_valve(cfg: Dict[str, Any]) -> Optional[int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg.get("valves"), dict) else {}
    for key in ("nitrogen_purge_source", "n2_purge_source", "nitrogen_source", "n2_source"):
        value = _as_optional_int(valves_cfg.get(key))
        if value is not None:
            return value
    purge_cfg = cfg.get("workflow", {}).get("nitrogen_purge", {})
    if isinstance(purge_cfg, dict):
        for key in ("source_valve", "logical_valve"):
            value = _as_optional_int(purge_cfg.get(key))
            if value is not None:
                return value
    return None


def _unique_ints(values: Iterable[Any]) -> List[int]:
    result: List[int] = []
    seen = set()
    for value in values:
        iv = _as_optional_int(value)
        if iv is None or iv in seen:
            continue
        seen.add(iv)
        result.append(iv)
    return result


def _build_nitrogen_purge_open_valves(
    runner: CalibrationRunner,
    point: CalibrationPoint,
) -> List[int]:
    """Build a diagnostic N2 purge route without opening the target CO2 source."""

    n2_source = _nitrogen_purge_source_valve(runner.cfg)
    if n2_source is None:
        return []
    managed_reader = getattr(runner, "_managed_valves", None)
    if callable(managed_reader):
        managed = {_as_optional_int(value) for value in managed_reader()}
        if int(n2_source) not in managed:
            raise RuntimeError(
                "N2_PURGE_SOURCE_NOT_MANAGED:"
                f"logical_valve={n2_source}; add it to valves.relay_map"
            )
    co2_source = _as_optional_int(runner._source_valve_for_point(point))
    if co2_source is not None and int(n2_source) == int(co2_source):
        raise RuntimeError(
            f"N2_PURGE_SOURCE_CONFLICTS_WITH_CO2_SOURCE:logical_valve={n2_source}"
        )
    base = [
        valve
        for valve in runner._co2_open_valves(point, include_total_valve=True)
        if _as_optional_int(valve) != co2_source
    ]
    return _unique_ints([*base, n2_source])


def _read_optional_float(device: Any, method_name: str) -> Optional[float]:
    if device is None:
        return None
    method = getattr(device, method_name, None)
    if not callable(method):
        return None
    try:
        value = method()
    except Exception:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _read_dewpoint_snapshot(device: Any) -> Dict[str, Any]:
    if device is None:
        return {}
    reader = getattr(device, "get_current_fast", None)
    if callable(reader):
        try:
            return dict(reader(timeout_s=0.5) or {})
        except Exception:
            return {}
    reader = getattr(device, "read", None)
    if callable(reader):
        try:
            return dict(reader() or {})
        except Exception:
            return {}
    return {}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def _write_purge_trace(
    path: Path,
    *,
    devices: Dict[str, Any],
    open_valves: List[int],
    purge_s: float,
    interval_s: float,
    max_pressure_hpa: float,
    transient_grace_s: float = 30.0,
    hard_limit_hpa: float = 1300.0,
) -> List[Dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    rows: List[Dict[str, Any]] = []
    fields = [
        "ts",
        "elapsed_s",
        "open_valves",
        "pace_pressure_hpa",
        "com22_pressure_hpa",
        "dewpoint_c",
        "dewpoint_temp_c",
        "dewpoint_rh_pct",
        "pressure_limit_hpa",
        "pressure_hard_limit_hpa",
        "pressure_over_limit_elapsed_s",
        "pressure_transient_status",
    ]
    over_limit_start: Optional[float] = None
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        while time.time() - start < max(0.0, float(purge_s)):
            dew = _read_dewpoint_snapshot(devices.get("dewpoint"))
            pace_pressure = _read_optional_float(devices.get("pace"), "read_pressure")
            gauge_pressure = _read_optional_float(devices.get("pressure_gauge"), "read_pressure")
            now = time.time()
            pressure_values = [
                float(value)
                for value in (pace_pressure, gauge_pressure)
                if value is not None
            ]
            pressure_transient_status = "in_limit"
            over_elapsed = 0.0
            for label, value in (("PACE", pace_pressure), ("COM22", gauge_pressure)):
                if value is not None and float(value) > float(hard_limit_hpa):
                    raise RuntimeError(
                        f"OPEN_FLOW_PRESSURE_HARD_LIMIT_EXCEEDED:{label}={float(value):.3f}hPa"
                    )
            if pressure_values and max(pressure_values) > float(max_pressure_hpa):
                if over_limit_start is None:
                    over_limit_start = now
                over_elapsed = max(0.0, now - over_limit_start)
                pressure_transient_status = "transient_over_limit"
                if over_elapsed > max(0.0, float(transient_grace_s)):
                    max_label, max_value = max(
                        (
                            (label, float(value))
                            for label, value in (("PACE", pace_pressure), ("COM22", gauge_pressure))
                            if value is not None
                        ),
                        key=lambda item: item[1],
                    )
                    raise RuntimeError(
                        "OPEN_FLOW_PRESSURE_PERSISTENT_LIMIT_EXCEEDED:"
                        f"{max_label}={max_value:.3f}hPa;"
                        f"elapsed_s={over_elapsed:.3f};limit={float(max_pressure_hpa):.3f}hPa"
                    )
            else:
                over_limit_start = None
            row = {
                "ts": datetime.now().isoformat(timespec="milliseconds"),
                "elapsed_s": round(now - start, 3),
                "open_valves": ",".join(str(v) for v in open_valves),
                "pace_pressure_hpa": pace_pressure,
                "com22_pressure_hpa": gauge_pressure,
                "dewpoint_c": dew.get("dewpoint_c"),
                "dewpoint_temp_c": dew.get("temp_c"),
                "dewpoint_rh_pct": dew.get("rh_pct"),
                "pressure_limit_hpa": float(max_pressure_hpa),
                "pressure_hard_limit_hpa": float(hard_limit_hpa),
                "pressure_over_limit_elapsed_s": round(over_elapsed, 3),
                "pressure_transient_status": pressure_transient_status,
            }
            writer.writerow(row)
            rows.append(
                {
                    "timestamp": row["ts"],
                    "phase_elapsed_s": row["elapsed_s"],
                    "elapsed_s": row["elapsed_s"],
                    "phase": "open_flow_purge",
                    "open_valves": row["open_valves"],
                    "controller_vent_state": "VENT_ON",
                    "controller_pressure_hpa": pace_pressure,
                    "gauge_pressure_hpa": gauge_pressure,
                    "dewpoint_c": row["dewpoint_c"],
                    "dewpoint_temp_c": row["dewpoint_temp_c"],
                    "dewpoint_rh_pct": row["dewpoint_rh_pct"],
                    "pressure_transient_status": pressure_transient_status,
                }
            )
            handle.flush()
            remain = float(purge_s) - (time.time() - start)
            time.sleep(min(max(0.2, float(interval_s)), max(0.0, remain)))
    return rows


def _write_route_timing(
    path: Path,
    *,
    run_id: str,
    co2_source_ppm: float,
    co2_group: str,
    route_opened: bool,
    co2_route_opened_at: Optional[str],
    sample_window_started_at: Optional[str],
    sample_window_ended_at: Optional[str],
    co2_route_closed_at: Optional[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sampling_before_route_close = False
    if sample_window_ended_at and co2_route_closed_at:
        sampling_before_route_close = sample_window_ended_at <= co2_route_closed_at
    payload = {
        "schema_version": "v1_5_formal_open_flow_route_timing_v0",
        "run_id": run_id,
        "co2_source_ppm": float(co2_source_ppm),
        "co2_group": str(co2_group),
        "route_opened": bool(route_opened),
        "co2_route_opened_at": co2_route_opened_at,
        "sample_window_started_at": sample_window_started_at,
        "sample_window_ended_at": sample_window_ended_at,
        "co2_route_closed_at": co2_route_closed_at,
        "sampling_before_route_close": bool(sampling_before_route_close),
        "physical_meaning": (
            "The CO2 route remains open during the formal sample window; "
            "route close is a post-sampling cleanup action."
        ),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _write_machine_readable_samples(run_dir: Path, rows: List[Dict[str, Any]]) -> Dict[str, str]:
    jsonl_path = run_dir / "samples_machine_readable.jsonl"
    csv_path = run_dir / "samples_machine_readable.csv"
    with jsonl_path.open("w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write(json.dumps({key: _json_safe(value) for key, value in row.items()}, ensure_ascii=False) + "\n")

    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            text = str(key)
            if text not in fieldnames:
                fieldnames.append(text)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: (
                        value
                        if value is None or isinstance(value, (str, int, float, bool))
                        else json.dumps(value, ensure_ascii=False, default=str)
                    )
                    for key, value in row.items()
                }
            )
    return {"jsonl": str(jsonl_path), "csv": str(csv_path)}


def _write_formal_evidence_bundle_manifest(
    run_dir: Path,
    *,
    run_id: str,
    route_kind: str,
    require_complete: bool = False,
) -> Path:
    """Bind one formal point's required artifacts without mutating source files."""

    root = run_dir.resolve()
    required = FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS.get(str(route_kind).lower())
    if not required:
        raise ValueError(f"unsupported_formal_evidence_route:{route_kind}")
    artifacts: list[dict[str, Any]] = []
    missing_roles: list[str] = []
    for role, filename in sorted(required.items()):
        path = root / filename
        if not path.is_file():
            missing_roles.append(role)
            continue
        artifacts.append(
            {
                "role": role,
                "filename": filename,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    canonical = {
        "schema_version": FORMAL_EVIDENCE_BUNDLE_SCHEMA,
        "run_id": str(run_id),
        "route_kind": str(route_kind).lower(),
        "identity_contract": "immutable_claim_runtime_run_id_and_sha256_bundle",
        "required_roles": sorted(required),
        "artifacts": artifacts,
        "missing_required_roles": sorted(missing_roles),
        "bundle_complete": not missing_roles,
    }
    rendered = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    payload = {
        **canonical,
        "bundle_sha256": hashlib.sha256(rendered).hexdigest(),
    }
    manifest_path = root / FORMAL_EVIDENCE_BUNDLE_FILENAME
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if require_complete and missing_roles:
        raise RuntimeError(
            "formal_evidence_bundle_incomplete:" + ",".join(sorted(missing_roles))
        )
    return manifest_path


def _enter_continuous_atmosphere(
    pace: Any,
    *,
    hold_interval_s: float = FORMAL_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S,
) -> None:
    if pace is None:
        return
    enter = getattr(pace, "enter_atmosphere_mode", None)
    if callable(enter):
        enter(timeout_s=30.0, poll_s=0.25, hold_open=True, hold_interval_s=hold_interval_s)


def _stop_continuous_atmosphere(pace: Any) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "action": "stop_continuous_atmosphere",
        "device_present": pace is not None,
        "stop_hold": {"supported": False, "ok": False},
        "exit_atmosphere": {"supported": False, "ok": False},
        "errors": [],
    }
    if pace is None:
        report["status"] = "fail"
        report["errors"].append("pace_not_configured")
        report["ok"] = False
        return report
    stop = getattr(pace, "stop_atmosphere_hold", None)
    if callable(stop):
        report["stop_hold"]["supported"] = True
        try:
            stop(timeout_s=2.0)
            report["stop_hold"]["ok"] = True
        except Exception as exc:
            report["errors"].append(f"stop_atmosphere_hold_failed:{exc}")
    else:
        report["errors"].append("stop_atmosphere_hold_not_supported")
    enter = getattr(pace, "enter_atmosphere_mode", None)
    if callable(enter):
        report["exit_atmosphere"]["supported"] = True
        try:
            enter(timeout_s=30.0, poll_s=0.25, hold_open=False)
            report["exit_atmosphere"]["ok"] = True
        except Exception as exc:
            report["errors"].append(f"exit_atmosphere_mode_failed:{exc}")
    else:
        report["errors"].append("enter_atmosphere_mode_not_supported")
    report["ok"] = not report["errors"]
    report["status"] = "pass" if report["ok"] else "fail"
    return report


def _write_shutdown_status_to_sidecar(
    sidecar_path: Path,
    shutdown_status: Dict[str, Any],
) -> None:
    with sidecar_path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"sidecar_metadata_not_object:{sidecar_path}")
    payload["physical_shutdown_status"] = shutdown_status
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _wait_open_flow_co2_dewpoint_gate(
    runner: CalibrationRunner,
    point: CalibrationPoint,
    *,
    purge_s: float,
    purge_begin_wall_s: float,
    purge_end_wall_s: float,
    base_soak_dewpoint_rows: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    enabled = getattr(runner, "_gas_route_dewpoint_gate_enabled", lambda: False)
    if not bool(enabled()):
        return True
    wait_gate = getattr(runner, "_wait_co2_route_dewpoint_gate_before_seal")
    return bool(
        wait_gate(
            point,
            base_soak_s=float(purge_s),
            log_context="open-flow sidecar after minimum purge",
            base_soak_begin_wall_s=purge_begin_wall_s,
            base_soak_end_wall_s=purge_end_wall_s,
            base_soak_dewpoint_rows=list(base_soak_dewpoint_rows or []),
        )
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.no_prompt:
        _log("Refusing to run real open-flow sampling without --no-prompt in this sidecar tool.")
        return 2

    cfg = load_config(args.config)
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        output_dir=args.output_dir,
        sample_count=args.sample_count,
        sample_interval_s=args.sample_interval_s,
        sensor_read_interval_s=args.sensor_read_interval_s,
        min_valid_analyzers=args.min_valid_analyzers,
        analyzer_acquisition=args.analyzer_acquisition,
        allow_ftd_write=args.allow_ftd_write,
        analyzer_gate_required_labels=_parse_label_list(args.analyzer_gate_required_labels),
        analyzer_gate_prefer_all_stable_grace_s=args.analyzer_gate_prefer_all_stable_grace_s,
        co2_ratio_f_preseal_tol=args.co2_ratio_f_preseal_tol,
        co2_ratio_f_preseal_window_s=args.co2_ratio_f_preseal_window_s,
        co2_ratio_f_preseal_timeout_s=args.co2_ratio_f_preseal_timeout_s,
        co2_ratio_f_preseal_min_samples=args.co2_ratio_f_preseal_min_samples,
        co2_ratio_f_preseal_policy=args.co2_ratio_f_preseal_policy,
        gas_route_dewpoint_gate_enabled=args.gas_route_dewpoint_gate_enabled,
        gas_route_dewpoint_gate_policy=args.gas_route_dewpoint_gate_policy,
        gas_route_dewpoint_require_dry_enough=args.gas_route_dewpoint_require_dry_enough,
        gas_route_dewpoint_dry_enough_c=args.gas_route_dewpoint_dry_enough_c,
        gas_route_dewpoint_gate_max_total_wait_s=args.gas_route_dewpoint_gate_max_total_wait_s,
        gas_route_dewpoint_gate_window_s=args.gas_route_dewpoint_gate_window_s,
        gas_route_dewpoint_gate_tail_span_max_c=args.gas_route_dewpoint_gate_tail_span_max_c,
        gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s=(
            args.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s
        ),
        gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c=(
            args.gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c
        ),
    )
    if args.n2_purge_source_valve is not None:
        runtime_cfg.setdefault("valves", {})["nitrogen_purge_source"] = int(args.n2_purge_source_valve)
    runtime_cfg.setdefault("metadata", {})["nitrogen_prepurge_enabled"] = bool(
        float(args.n2_prepurge_s) > 0.0
    )
    runtime_cfg["metadata"]["nitrogen_prepurge_s"] = max(0.0, float(args.n2_prepurge_s))
    runtime_cfg["metadata"]["nitrogen_purge_source_valve"] = _nitrogen_purge_source_valve(runtime_cfg)
    output_dir = Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"formal_open_flow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    runtime_cfg.setdefault("metadata", {})["run_id"] = run_id
    runtime_cfg["metadata"]["evidence_identity_contract"] = (
        "immutable_claim_runtime_run_id_and_sha256_bundle"
    )
    logger = RunLogger(
        output_dir,
        run_id=run_id,
        cfg=runtime_cfg,
        immutable_run_dir=True,
    )
    catalog_path = (
        Path(args.reference_source_catalog).resolve()
        if args.reference_source_catalog
        else Path(args.config).resolve().parent / "v1_5_reference_source_catalog.json"
    )
    reference_source_record = _build_co2_reference_source_record(
        catalog_path,
        run_id=run_id,
        co2_source_ppm=float(args.co2_source_ppm),
        certificate_co2_ppm=args.certificate_co2_ppm,
        reference_asset_id=args.reference_asset_id,
    )
    reference_source_path = _write_formal_reference_source_record(
        logger.run_dir,
        reference_source_record,
    )
    selected_reference_asset = reference_source_record.get("selected_asset")
    if not isinstance(selected_reference_asset, dict):
        selected_reference_asset = {}
    runtime_cfg["metadata"]["reference_source_status"] = reference_source_record[
        "reference_source_status"
    ]
    runtime_cfg["metadata"]["reference_asset_id"] = selected_reference_asset.get("asset_id")
    runtime_cfg["metadata"]["reference_source_catalog_sha256"] = reference_source_record.get(
        "catalog_sha256"
    )
    runtime_snapshot_path = logger.run_dir / "runtime_config_snapshot.json"
    runtime_snapshot_path.write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    devices: Dict[str, Any] = {}
    runner: Optional[CalibrationRunner] = None
    route_opened = False
    n2_purge_path: Optional[Path] = None
    n2_open_valves: List[int] = []
    route_timing_path = logger.run_dir / "formal_open_flow_route_timing.json"
    co2_route_opened_at: Optional[str] = None
    sample_window_started_at: Optional[str] = None
    sample_window_ended_at: Optional[str] = None
    co2_route_closed_at: Optional[str] = None
    sidecar_metadata_path: Optional[Path] = None
    formal_sampling_completed = False
    finalization_failures: List[str] = []

    try:
        if reference_source_record["reference_source_status"] != "pass":
            raise RuntimeError(
                "FORMAL_REFERENCE_SOURCE_GATE_FAILED:"
                + ",".join(reference_source_record.get("reasons") or ["unknown"])
            )
        point = _build_open_flow_point(
            temp_c=args.temp,
            co2_source_ppm=args.co2_source_ppm,
            co2_group=args.co2_group,
            pressure_target_hpa=args.pressure_target_hpa,
        )
        _log("V1.5 open-flow sidecar: no sealed pressure control, no OUTP control, no SENCO/ID writes.")
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        runner._configure_devices()
        runner._startup_preflight_reset()
        _defer_startup_mode2_disabled_analyzers(runner)
        if not _verify_v1_5_point_temperature_truth(
            runtime_cfg,
            devices,
            target_c=float(args.temp),
        ):
            raise RuntimeError("V1_5_POINT_TEMPERATURE_TRUTH_GATE_FAILED")

        pace = devices.get("pace")
        _enter_continuous_atmosphere(
            pace,
            hold_interval_s=FORMAL_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S,
        )
        if float(args.n2_prepurge_s) > 0.0:
            n2_open_valves = _build_nitrogen_purge_open_valves(runner, point)
            if not n2_open_valves:
                raise RuntimeError(
                    "N2_PURGE_SOURCE_NOT_CONFIGURED: set valves.nitrogen_purge_source "
                    "and relay_map for the logical valve, or pass --n2-purge-source-valve"
                )
            runner._apply_valve_states(n2_open_valves)
            route_opened = True
            n2_purge_path = logger.run_dir / "formal_open_flow_n2_prepurge_trace.csv"
            _log(
                "N2 diagnostic pre-purge start: "
                f"{float(args.n2_prepurge_s):g}s valves={n2_open_valves}"
            )
            _write_purge_trace(
                n2_purge_path,
                devices=devices,
                open_valves=n2_open_valves,
                purge_s=float(args.n2_prepurge_s),
                interval_s=float(args.purge_trace_interval_s),
                max_pressure_hpa=float(args.max_open_flow_pressure_hpa),
                transient_grace_s=float(args.open_flow_pressure_transient_grace_s),
                hard_limit_hpa=float(args.open_flow_pressure_safety_hard_limit_hpa),
            )
            _log(f"N2 diagnostic pre-purge trace saved: {n2_purge_path}")
        open_valves = runner._co2_open_valves(point, include_total_valve=True)
        runner._apply_valve_states(open_valves)
        route_opened = True
        co2_route_opened_at = _now_iso()
        _log(f"CO2 open-flow route opened: valves={open_valves}")
        _apply_certificate_target_after_valve_selection(point, args.certificate_co2_ppm)

        meta_path = logger.run_dir / "formal_open_flow_sidecar_metadata.json"
        sidecar_metadata_path = meta_path
        ftd_write_enabled = bool(runtime_cfg["metadata"].get("ftd_write_enabled", False))
        meta_path.write_text(
            json.dumps(
                {
                    "schema_version": "v1_5_formal_open_flow_sidecar_v0",
                    "run_id": run_id,
                    "co2_source_ppm": args.co2_source_ppm,
                    "co2_group": args.co2_group,
                    "certificate_co2_ppm": args.certificate_co2_ppm,
                    "certificate_uncertainty_ppm": args.certificate_uncertainty_ppm,
                    "reference_asset_id": selected_reference_asset.get("asset_id"),
                    "reference_value_source": selected_reference_asset.get(
                        "reference_value_source"
                    ),
                    "reference_source_record": str(reference_source_path),
                    "n2_prepurge_enabled": bool(float(args.n2_prepurge_s) > 0.0),
                    "n2_prepurge_s": max(0.0, float(args.n2_prepurge_s)),
                    "n2_purge_source_valve": _nitrogen_purge_source_valve(runtime_cfg),
                    "n2_purge_open_valves": n2_open_valves,
                    "n2_purge_trace": str(n2_purge_path) if n2_purge_path else None,
                    "open_valves": open_valves,
                    "actual_purge_s": float(args.purge_s),
                    "minimum_purge_s": float(args.minimum_purge_s),
                    "route_open_until_sample_end": True,
                    "pace_mode": "continuous_atmosphere_hold",
                    "continuous_atmosphere_hold_scope": "open_flow_purge_and_sampling_only",
                    "idle_continuous_atmosphere_hold": False,
                    "gas_route_dewpoint_gate_enabled": bool(
                        runtime_cfg.get("workflow", {})
                        .get("stability", {})
                        .get("gas_route_dewpoint_gate_enabled", False)
                    ),
                    "gas_route_dewpoint_gate_policy": (
                        runtime_cfg.get("workflow", {})
                        .get("stability", {})
                        .get("gas_route_dewpoint_gate_policy")
                    ),
                    "gas_route_dewpoint_gate_dry_enough_c": (
                        runtime_cfg.get("workflow", {})
                        .get("stability", {})
                        .get("gas_route_dewpoint_gate_dry_enough_c")
                    ),
                    "analyzer_acquisition_policy": runtime_cfg["metadata"]["analyzer_acquisition_policy"],
                    "analyzer_stream_native_hz": runtime_cfg["metadata"].get("analyzer_stream_native_hz"),
                    "formal_sample_anchor_interval_s": runtime_cfg["metadata"].get(
                        "formal_sample_anchor_interval_s"
                    ),
                    "formal_sample_decimation": runtime_cfg["metadata"].get("formal_sample_decimation"),
                    "ftd_write_enabled": ftd_write_enabled,
                    "sealed_pressure_control": False,
                    "writes_senco": False,
                    "writes_device_id": False,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        purge_path = logger.run_dir / "formal_open_flow_purge_trace.csv"
        _log(f"Open-flow purge start: {args.purge_s:g}s")
        purge_begin_wall_s = time.time()
        purge_dewpoint_rows = _write_purge_trace(
            purge_path,
            devices=devices,
            open_valves=open_valves,
            purge_s=float(args.purge_s),
            interval_s=float(args.purge_trace_interval_s),
            max_pressure_hpa=float(args.max_open_flow_pressure_hpa),
            transient_grace_s=float(args.open_flow_pressure_transient_grace_s),
            hard_limit_hpa=float(args.open_flow_pressure_safety_hard_limit_hpa),
        )
        purge_end_wall_s = time.time()
        _log(f"Open-flow purge trace saved: {purge_path}")

        if not args.skip_stability_gate:
            if not _wait_open_flow_co2_dewpoint_gate(
                runner,
                point,
                purge_s=float(args.purge_s),
                purge_begin_wall_s=purge_begin_wall_s,
                purge_end_wall_s=purge_end_wall_s,
                base_soak_dewpoint_rows=purge_dewpoint_rows,
            ):
                _log("Open-flow route dewpoint gate failed.")
                return 1
            if not runner._wait_co2_preseal_primary_sensor_gate(point):
                _log("Open-flow analyzer stability gate failed.")
                return 1

        runner._set_point_runtime_fields(
            point,
            phase="co2",
            formal_open_flow_sidecar=True,
            route_open_until_sample_end=True,
            gas_route_open_until_sample_end=True,
            actual_purge_s=float(args.purge_s),
            minimum_purge_s=float(args.minimum_purge_s),
            open_flow_purge_elapsed_s=float(args.purge_s),
            sample_readiness_basis=(
                "minimum_purge_plus_dry_enough_dewpoint_gate_plus_ratio_stability_plus_best_live_stable_window"
            ),
            standard_gas_certificate_value_ppm=args.certificate_co2_ppm,
            standard_gas_certificate_uncertainty_ppm=args.certificate_uncertainty_ppm,
            sealed_pressure_control=False,
        )
        sample_window_started_at = _now_iso()
        runner._sample_and_log(point, phase="co2", point_tag=f"open_flow_{int(args.co2_source_ppm)}ppm")
        sample_window_ended_at = _now_iso()
        machine_sample_paths = _write_machine_readable_samples(logger.run_dir, runner._all_samples)
        tables = analyze_sample_rows(runner._all_samples, cfg=runtime_cfg, gas="co2", modes=("current",))
        outputs = write_validation_report(
            logger.run_dir,
            prefix="formal_open_flow_sampling_validation",
            metadata=ValidationMetadata(
                tool_name="run_v1_5_formal_open_flow_sampling",
                analyzers=sorted(
                    {
                        str(row.get("Analyzer") or "")
                        for row in tables["frame_quality_summary"]
                        if row.get("Analyzer")
                    }
                ),
                input_paths=[
                    str(logger.samples_path),
                    str(machine_sample_paths["csv"]),
                    str(machine_sample_paths["jsonl"]),
                    str(logger.points_path),
                    str(runtime_snapshot_path),
                    str(meta_path),
                    str(reference_source_path),
                    str(purge_path),
                ]
                + ([str(n2_purge_path)] if n2_purge_path else []),
                output_dir=str(logger.run_dir),
                config_path=str(Path(args.config).resolve()),
                config_summary={
                    "co2_source_ppm": float(args.co2_source_ppm),
                    "co2_group": str(args.co2_group),
                    "reference_asset_id": selected_reference_asset.get("asset_id"),
                    "reference_value_source": selected_reference_asset.get(
                        "reference_value_source"
                    ),
                    "n2_prepurge_enabled": bool(float(args.n2_prepurge_s) > 0.0),
                    "n2_prepurge_s": max(0.0, float(args.n2_prepurge_s)),
                    "n2_purge_source_valve": _nitrogen_purge_source_valve(runtime_cfg),
                    "n2_purge_open_valves": n2_open_valves,
                    "sample_count": int(args.sample_count),
                    "sample_interval_s": float(args.sample_interval_s),
                    "purge_s": float(args.purge_s),
                    "minimum_purge_s": float(args.minimum_purge_s),
                    "route_open_until_sample_end": True,
                    "max_open_flow_pressure_hpa": float(args.max_open_flow_pressure_hpa),
                    "sensor_read_interval_s": float(args.sensor_read_interval_s),
                    "min_valid_analyzers": int(args.min_valid_analyzers),
                    "analyzer_acquisition_policy": runtime_cfg["metadata"]["analyzer_acquisition_policy"],
                    "analyzer_stream_native_hz": runtime_cfg["metadata"].get("analyzer_stream_native_hz"),
                    "formal_sample_anchor_interval_s": runtime_cfg["metadata"].get(
                        "formal_sample_anchor_interval_s"
                    ),
                    "formal_sample_decimation": runtime_cfg["metadata"].get("formal_sample_decimation"),
                    "ftd_write_enabled": ftd_write_enabled,
                    "idle_continuous_atmosphere_hold": False,
                    "continuous_atmosphere_hold_scope": "open_flow_purge_and_sampling_only",
                    "analyzer_chamber_temp_span_c": runtime_cfg["workflow"]
                    .get("stability", {})
                    .get("temperature", {})
                    .get("analyzer_chamber_temp_span_c"),
                    "sealed_pressure_control": False,
                },
                notes=[
                    "PACE was held open to atmosphere; OUTP pressure control was not used.",
                    "CO2 route valves were closed after sampling; no SENCO or ID writes are performed.",
                    "This sidecar samples a single requested open-flow CO2 source and does not run the legacy sealed pressure sweep.",
                    "Optional N2 pre-purge is diagnostic only and is not a formal standard-gas calibration point.",
                ],
            ),
            tables=tables,
        )
        _log(f"Formal open-flow sampling validation saved: {outputs['workbook']}")
        formal_sampling_completed = True
    except Exception as exc:
        _log(f"Formal open-flow sampling failed: {exc}")
        return 1
    finally:
        route_close_status: Dict[str, Any] = {
            "required": bool(route_opened),
            "attempted": False,
            "ok": not route_opened,
            "status": "not_required" if not route_opened else "pending",
        }
        if runner is not None and route_opened:
            route_close_status["attempted"] = True
            try:
                runner._apply_valve_states([])
                co2_route_closed_at = _now_iso()
                route_close_status["ok"] = True
                route_close_status["status"] = "pass"
                _log("CO2 open-flow route closed")
            except Exception as exc:
                route_close_status["ok"] = False
                route_close_status["status"] = "fail"
                route_close_status["error"] = str(exc)
                finalization_failures.append(f"route_close_failed:{exc}")
                _log(f"CO2 route close failed: {exc}")
        try:
            _write_route_timing(
                route_timing_path,
                run_id=run_id,
                co2_source_ppm=float(args.co2_source_ppm),
                co2_group=str(args.co2_group),
                route_opened=bool(route_opened),
                co2_route_opened_at=co2_route_opened_at,
                sample_window_started_at=sample_window_started_at,
                sample_window_ended_at=sample_window_ended_at,
                co2_route_closed_at=co2_route_closed_at,
            )
        except Exception as exc:
            finalization_failures.append(f"route_timing_write_failed:{exc}")
            _log(f"CO2 route timing evidence write failed: {exc}")
        pace_stop_status = _stop_continuous_atmosphere(devices.get("pace"))
        if formal_sampling_completed and pace_stop_status.get("ok") is not True:
            finalization_failures.append(
                "pace_atmosphere_stop_failed:"
                + ",".join(pace_stop_status.get("errors") or ["unknown"])
            )
        device_close_status: Dict[str, Any] = {
            "status": "best_effort_pending",
            "fully_observable": False,
            "note": "Shared device closer preserves V1 behavior and does not expose per-device close errors.",
        }
        try:
            _close_devices(devices)
            device_close_status["status"] = "best_effort_invoked"
        except Exception as exc:
            device_close_status["status"] = "call_failed"
            device_close_status["error"] = str(exc)
            finalization_failures.append(f"device_close_call_failed:{exc}")
        physical_failures = [
            reason
            for reason in finalization_failures
            if reason.startswith(
                (
                    "route_close_failed:",
                    "pace_atmosphere_stop_failed:",
                    "device_close_call_failed:",
                )
            )
        ]
        physical_shutdown_status = {
            "schema_version": "v1_5_formal_physical_shutdown_status_v0",
            "route_kind": "co2",
            "route_close": route_close_status,
            "pace_atmosphere_stop": pace_stop_status,
            "device_transport_close": device_close_status,
            "critical_failures": physical_failures,
            "overall_status": "fail" if physical_failures else "pass",
        }
        if sidecar_metadata_path is not None and sidecar_metadata_path.is_file():
            try:
                _write_shutdown_status_to_sidecar(
                    sidecar_metadata_path,
                    physical_shutdown_status,
                )
            except Exception as exc:
                finalization_failures.append(f"shutdown_status_write_failed:{exc}")
                _log(f"CO2 physical shutdown status write failed: {exc}")
        try:
            bundle_path = _write_formal_evidence_bundle_manifest(
                logger.run_dir,
                run_id=run_id,
                route_kind="co2",
                require_complete=formal_sampling_completed,
            )
            _log(f"CO2 formal evidence bundle saved: {bundle_path}")
        except Exception as exc:
            finalization_failures.append(f"evidence_bundle_failed:{exc}")
            _log(f"CO2 formal evidence bundle write failed: {exc}")
        try:
            logger.close()
        except Exception:
            pass
    if finalization_failures:
        _log(
            "CO2 formal sampling evidence finalization failed: "
            + " | ".join(finalization_failures)
        )
    return _formal_sampling_completion_code(
        sampling_completed=formal_sampling_completed,
        finalization_failures=finalization_failures,
    )


if __name__ == "__main__":
    sys.exit(main())
