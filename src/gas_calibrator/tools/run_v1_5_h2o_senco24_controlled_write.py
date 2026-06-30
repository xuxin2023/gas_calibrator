"""Run controlled V1.5 H2O SENCO2 + SENCO4 paired writes.

This high-risk real-device tool writes only reviewed H2O coefficient groups:
SENCO2 for the H2O ratio polynomial terms and SENCO4 for temperature terms.
It never changes analyzer IDs, never clears coefficient groups, and never
controls PACE, valves, water routes, gas routes, or humidity generation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..devices import GasAnalyzer
from ..senco_format import senco_readback_matches
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_co2_senco13_controlled_write as pair_base
from . import run_v1_5_co2_senco1_controlled_write as base
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO2_SENCO4_V1_5_H2O_PAIR"
PAIR_WRITE_PAYLOAD_WIDTH = 6
H2O_SENCO24_ALGORITHM_OLD_RATIO_TEMPERATURE = "old_ratio_temperature"
H2O_SENCO24_ALGORITHM_NEW_ABSORPTION = "new_absorption"
H2O_SENCO24_ALGORITHMS = (
    H2O_SENCO24_ALGORITHM_OLD_RATIO_TEMPERATURE,
    H2O_SENCO24_ALGORITHM_NEW_ABSORPTION,
)
NEW_ABSORPTION_CONTRACT_MARKERS = ("new", "absorption")


def _zero_tail(values: Sequence[float], start: int, *, atol: float = pair_base.SECONDARY_PRESSURE_SLOT_ZERO_ATOL) -> bool:
    try:
        return all(abs(float(value)) <= float(atol) for value in values[int(start) :])
    except Exception:
        return False


def _contract_text(*rows: Mapping[str, Any]) -> str:
    fields = (
        "senco24_main_chain_contract",
        "candidate_contract",
        "model_formula",
        "coefficient_order",
        "fit_strategy",
        "diagnosis",
    )
    chunks: List[str] = []
    for row in rows:
        for field in fields:
            value = row.get(field)
            if value is not None:
                chunks.append(str(value))
    return " ".join(chunks).strip().lower()


def _new_absorption_contract_reviewed(
    row: Mapping[str, Any],
    policy: Mapping[str, Any],
    diag: Mapping[str, Any],
) -> bool:
    del diag
    text = _contract_text(row, policy)
    if not all(marker in text for marker in NEW_ABSORPTION_CONTRACT_MARKERS):
        return False
    order = str(row.get("coefficient_order") or policy.get("coefficient_order") or "").strip().lower()
    return order in {"", "ascending_constant_first", "constant_first"}


def _payload_slots_supported(
    *,
    primary_target: Sequence[float],
    secondary_target: Sequence[float],
    h2o_senco24_algorithm: str,
) -> bool:
    if len(primary_target) != PAIR_WRITE_PAYLOAD_WIDTH or len(secondary_target) != PAIR_WRITE_PAYLOAD_WIDTH:
        return False
    if h2o_senco24_algorithm == H2O_SENCO24_ALGORITHM_NEW_ABSORPTION:
        # New algorithm H2O absorption contract:
        #   SENCO2 = lnR0 b0..b4, reserved zero tail
        #   SENCO4 = k b0..b3, reserved zero tail
        return _zero_tail(primary_target, 5) and _zero_tail(secondary_target, 4)
    return pair_base._secondary_pressure_target_slots_zero(secondary_target)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _csv_by_device(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    return {base._device_id(row.get("analyzer_device_id")): dict(row) for row in base._read_csv(path)}


def _snapshot_for_device(snapshot: Mapping[str, Any], device_id: str) -> Dict[str, Any]:
    return dict(snapshot.get(base._device_id(device_id)) or {})


def _neutral_senco6(values: Sequence[float], *, atol: float = 0.05) -> bool:
    return len(values) >= 2 and abs(float(values[0])) <= float(atol) and abs(float(values[1]) - 1.0) <= float(atol)


def _review_read_min_count(values: Sequence[float]) -> int:
    """Read enough live values to prove a reviewed snapshot without requiring visible zero tails."""

    count = len(list(values))
    if count <= 2:
        return count
    return min(4, count)


def _supported_rows(
    *,
    review_dir: Path,
    old_snapshot: Mapping[str, Any],
    allow_review_required: bool = False,
    allow_separate_senco6_layer_review: bool = False,
    h2o_senco24_algorithm: str = H2O_SENCO24_ALGORITHM_OLD_RATIO_TEMPERATURE,
) -> List[Dict[str, Any]]:
    payload_rows = base._read_csv(review_dir / "h2o_senco24_payload_preview.csv")
    policy_by_device = _csv_by_device(review_dir / "h2o_senco24_device_policy.csv")
    diag_by_device = _csv_by_device(review_dir / "h2o_senco24_output_diagnostics.csv")
    out: List[Dict[str, Any]] = []

    for row in payload_rows:
        device_id = base._device_id(row.get("analyzer_device_id"))
        if str(row.get("component") or "").strip().lower() != "h2o":
            continue
        if str(row.get("primary_senco") or "").strip().upper() != "SENCO2":
            continue
        if str(row.get("secondary_senco") or "").strip().upper() != "SENCO4":
            continue
        policy = policy_by_device.get(device_id, {})
        if str(policy.get("blocked_reasons") or "").strip():
            continue
        status = str(policy.get("candidate_status") or "").strip()
        diag = diag_by_device.get(device_id, {})
        if status == "candidate_ratio_fit_available_but_final_output_blocked":
            diagnosis = str(diag.get("diagnosis") or "")
            if diagnosis != "final_h2o_output_pinned_with_neutral_senco6" and not bool(
                allow_separate_senco6_layer_review
            ):
                continue
        elif status == "candidate_fit_review_required" and bool(allow_review_required):
            pass
        elif not status.startswith("candidate_fit_ready"):
            continue

        primary_target = base._parse_values(row.get("senco2_payload_values_json"))
        secondary_target = base._parse_values(row.get("senco4_payload_values_json"))
        snapshot = _snapshot_for_device(old_snapshot, device_id)
        old_primary = base._parse_values(json.dumps(snapshot.get("GETCO2_before") or []))
        old_secondary = base._parse_values(json.dumps(snapshot.get("GETCO4_before") or []))
        old_senco6 = base._parse_values(json.dumps(snapshot.get("GETCO6_before") or []))
        if h2o_senco24_algorithm == H2O_SENCO24_ALGORITHM_NEW_ABSORPTION and not _new_absorption_contract_reviewed(
            row, policy, diag
        ):
            continue
        if not _payload_slots_supported(
            primary_target=primary_target,
            secondary_target=secondary_target,
            h2o_senco24_algorithm=h2o_senco24_algorithm,
        ):
            continue
        # Some firmware revisions omit trailing zero coefficient slots when
        # reading old GETCO4 values.  The live snapshot matcher below already
        # treats missing/extra zero tails as equivalent, so require the
        # physically meaningful leading slots instead of rejecting the device.
        if len(old_primary) < 4 or len(old_secondary) < 4:
            continue
        old_senco6_is_neutral = _neutral_senco6(old_senco6)
        if not old_senco6_is_neutral and not bool(allow_separate_senco6_layer_review):
            continue

        item = dict(row)
        item["_primary_target_values"] = primary_target
        item["_secondary_target_values"] = secondary_target
        item["_old_primary_values"] = old_primary
        item["_old_secondary_values"] = old_secondary
        item["_old_senco6_values"] = old_senco6
        item["_policy"] = policy
        item["_diagnostics"] = diag
        item["_senco6_separate_layer_reviewed"] = bool(
            allow_separate_senco6_layer_review and not old_senco6_is_neutral
        )
        item["_manual_review_required_override"] = (
            status == "candidate_fit_review_required" and bool(allow_review_required)
        )
        out.append(item)
    return out


def _select_targets(
    supported: Sequence[Mapping[str, Any]],
    *,
    selected_device_ids: Sequence[str],
    write_all_ready: bool,
) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in supported]
    if write_all_ready:
        return rows
    wanted = {base._device_id(item) for item in selected_device_ids if str(item or "").strip()}
    return [row for row in rows if base._device_id(row.get("analyzer_device_id")) in wanted]


def _verify_review_snapshot(
    ga: GasAnalyzer,
    group: int,
    reviewed: Sequence[float],
    *,
    readback_attempts: int,
    retry_delay_s: float,
    compare_atol: float,
) -> tuple[bool, List[float], str]:
    try:
        values = base._read_reviewed_group_values_with_retry(
            ga,
            int(group),
            reviewed,
            min_count=_review_read_min_count(reviewed),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=float(retry_delay_s),
            atol=float(compare_atol),
        )
    except Exception as exc:
        return False, [], str(exc)
    return True, list(values), ""


def _set_senco24_with_rollback(
    ga: GasAnalyzer,
    *,
    primary_target_values: Sequence[float],
    secondary_target_values: Sequence[float],
    old_primary_live: Sequence[float],
    old_secondary_live: Sequence[float],
    restore_mode: int,
    readback_attempts: int,
    retry_delay_s: float,
    post_write_settle_s: float,
    compare_atol: float,
    write_attempts: int,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": False,
        "mode_requested": 2,
        "restore_mode": int(restore_mode),
        "old_senco2_live": list(old_primary_live),
        "old_senco4_live": list(old_secondary_live),
        "target_senco2": list(primary_target_values),
        "target_senco4": list(secondary_target_values),
        "senco2_result": {},
        "senco4_result": {},
        "final_senco2_readback": [],
        "final_senco4_readback": [],
        "rollback_attempted": False,
        "rollback_senco2_confirmed": False,
        "rollback_senco4_confirmed": False,
        "failure_reason": "",
    }
    primary_attempted = False
    secondary_attempted = False
    try:
        if not ga.set_mode_with_ack(2, require_ack=True):
            raise RuntimeError("MODE=2 not acknowledged before SENCO2/SENCO4 write")
        base._sleep_gap(retry_delay_s)

        primary = pair_base._write_group_with_readback(
            ga,
            2,
            primary_target_values,
            readback_attempts=readback_attempts,
            retry_delay_s=retry_delay_s,
            post_write_settle_s=post_write_settle_s,
            compare_atol=compare_atol,
            write_attempts=write_attempts,
        )
        primary_attempted = True
        result["senco2_result"] = primary
        if primary.get("verify_status") != "success":
            raise RuntimeError(primary.get("failure_reason") or "SENCO2_VERIFY_FAILED")

        secondary = pair_base._write_group_with_readback(
            ga,
            4,
            secondary_target_values,
            readback_attempts=readback_attempts,
            retry_delay_s=retry_delay_s,
            post_write_settle_s=post_write_settle_s,
            compare_atol=compare_atol,
            write_attempts=write_attempts,
        )
        secondary_attempted = True
        result["senco4_result"] = secondary
        if secondary.get("verify_status") != "success":
            raise RuntimeError(secondary.get("failure_reason") or "SENCO4_VERIFY_FAILED")

        result["final_senco2_readback"] = base._read_group_values_with_retry(
            ga,
            2,
            min_count=len(list(primary_target_values)),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=retry_delay_s,
        )
        result["final_senco4_readback"] = base._read_group_values_with_retry(
            ga,
            4,
            min_count=len(list(secondary_target_values)),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=retry_delay_s,
        )
        if not senco_readback_matches(
            primary_target_values,
            result["final_senco2_readback"][: len(list(primary_target_values))],
            atol=float(compare_atol),
        ):
            raise RuntimeError("SENCO2_FINAL_READBACK_MISMATCH")
        if not senco_readback_matches(
            secondary_target_values,
            result["final_senco4_readback"][: len(list(secondary_target_values))],
            atol=float(compare_atol),
        ):
            raise RuntimeError("SENCO4_FINAL_READBACK_MISMATCH")
        result["ok"] = True
    except Exception as exc:
        result["failure_reason"] = str(exc)
        if primary_attempted or secondary_attempted:
            result["rollback_attempted"] = True
            try:
                ga.set_senco(4, *old_secondary_live)
                base._sleep_gap(post_write_settle_s)
                ok4, rb4, reason4 = _verify_review_snapshot(
                    ga,
                    4,
                    old_secondary_live,
                    readback_attempts=readback_attempts,
                    retry_delay_s=retry_delay_s,
                    compare_atol=compare_atol,
                )
                result["rollback_senco4_readback"] = rb4
                result["rollback_senco4_confirmed"] = bool(ok4)
                if not ok4:
                    result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO4_FAILED:{reason4}"
            except Exception as rollback_exc:
                result["rollback_senco4_confirmed"] = False
                result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO4_FAILED:{rollback_exc}"
            try:
                ga.set_senco(2, *old_primary_live)
                base._sleep_gap(post_write_settle_s)
                ok2, rb2, reason2 = _verify_review_snapshot(
                    ga,
                    2,
                    old_primary_live,
                    readback_attempts=readback_attempts,
                    retry_delay_s=retry_delay_s,
                    compare_atol=compare_atol,
                )
                result["rollback_senco2_readback"] = rb2
                result["rollback_senco2_confirmed"] = bool(ok2)
                if not ok2:
                    result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO2_FAILED:{reason2}"
            except Exception as rollback_exc:
                result["rollback_senco2_confirmed"] = False
                result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO2_FAILED:{rollback_exc}"
    finally:
        try:
            ga.set_mode_with_ack(int(restore_mode), require_ack=False)
        except Exception:
            pass
    return result


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 H2O SENCO2 + SENCO4 paired coefficient writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--review-dir", required=True, help="Directory containing H2O SENCO2/SENCO4 review artifacts.")
    parser.add_argument("--old-component-snapshot-json", required=True, help="Read-only GETCO2/GETCO4/GETCO6 snapshot JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to write.")
    parser.add_argument("--write-all-ready", action="store_true", help="Write every ready H2O SENCO2/SENCO4 candidate.")
    parser.add_argument(
        "--h2o-senco24-algorithm",
        choices=H2O_SENCO24_ALGORITHMS,
        default=H2O_SENCO24_ALGORITHM_OLD_RATIO_TEMPERATURE,
        help=(
            "Select the SENCO2/SENCO4 slot contract. old_ratio_temperature keeps the mature V1.5 "
            "pressure-slot-zero gate. new_absorption allows SENCO4 slot 4 to carry k3 only when "
            "the review artifacts explicitly declare a new absorption contract."
        ),
    )
    parser.add_argument(
        "--allow-review-required-candidates",
        action="store_true",
        help=(
            "Allow non-blocked candidate_fit_review_required rows after explicit reviewer/approver sign-off. "
            "This does not relax blocked rows or pressure-slot checks."
        ),
    )
    parser.add_argument(
        "--allow-separate-senco6-layer-review",
        action="store_true",
        help=(
            "Allow SENCO2/SENCO4 main-chain writes when GETCO6 is non-neutral only if SENCO6 has been "
            "reviewed as a separate final affine layer to be written after the main chain."
        ),
    )
    parser.add_argument("--enable-senco24-write", action="store_true", help="Required to write SENCO2 and SENCO4.")
    parser.add_argument("--operator-confirmation", default="", help=f"Must equal {CONFIRMATION_TEXT!r}.")
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--stop-on-failure", action="store_true", default=True)
    parser.add_argument("--continue-on-failure", dest="stop_on_failure", action="store_false")
    parser.add_argument("--identity-timeout-s", type=float, default=4.0)
    parser.add_argument("--readback-attempts", type=int, default=3)
    parser.add_argument("--write-attempts", type=int, default=2)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument("--post-write-settle-s", type=float, default=2.0)
    parser.add_argument("--compare-atol", type=float, default=1e-9)
    parser.add_argument("--pre-device-cooldown-s", type=float, default=5.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=10.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument("--restore-active-freq", action="store_true", default=True)
    parser.add_argument("--no-restore-active-freq", dest="restore_active_freq", action="store_false")
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=2.0)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=4)
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_database_sidecar(
    destination: Path,
    *,
    outputs: Mapping[str, Path],
    snapshot_path: Path,
    summary_rows: Sequence[Mapping[str, Any]],
    conclusion_rows: Sequence[Mapping[str, Any]],
    h2o_senco24_algorithm: str,
) -> Path:
    artifacts: List[Dict[str, Any]] = []
    artifact_paths: Dict[str, Path] = {key: Path(value) for key, value in outputs.items()}
    artifact_paths["old_getco2_getco4_getco6_snapshot_json"] = snapshot_path
    for key, path in sorted(artifact_paths.items()):
        if path.exists():
            artifacts.append(
                {
                    "output_key": key,
                    "artifact_role": "coefficient_snapshot" if "snapshot" in key else "coefficient_write_log",
                    "path": str(path.resolve()),
                    "sha256": _sha256_file(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    old_coefficients_hash = _sha256_file(snapshot_path) if snapshot_path.exists() else ""
    suggested_rows: List[Dict[str, Any]] = []
    for row in summary_rows:
        device_id = base._device_id(row.get("analyzer_device_id"))
        if h2o_senco24_algorithm == H2O_SENCO24_ALGORITHM_NEW_ABSORPTION:
            command_summary = (
                "SENCO2 and SENCO4 paired H2O coefficient write using the new absorption contract; "
                "SENCO2 carries lnR0(T1), SENCO4 carries k(T1), no route control, no device-ID write, no CLEARSENCO."
            )
        else:
            command_summary = (
                "SENCO2 and SENCO4 paired H2O coefficient write; pressure target slots remain frozen at zero; "
                "no route control, no device-ID write, no CLEARSENCO."
            )
        suggested_rows.append(
            {
                "db_table": "coefficient_write_events",
                "record_key": f"h2o_senco24_pair_write_{device_id}",
                "component": "h2o",
                "analyzer_device_id": device_id,
                "event_type": "h2o_senco2_senco4_paired_write",
                "status": str(row.get("status") or "unknown"),
                "approved_by": row.get("approver"),
                "old_coefficients_hash": old_coefficients_hash,
                "command_summary": command_summary,
                "readback_json": json.dumps(
                    {
                        "senco2": base._parse_values(row.get("senco2_readback")),
                        "senco4": base._parse_values(row.get("senco4_readback")),
                        "identity_before": row.get("identity_before", ""),
                        "identity_after": row.get("identity_after", ""),
                    },
                    ensure_ascii=False,
                ),
                "source_path": str(destination.resolve()),
            }
        )
    payload = {
        "schema": "v1_5_h2o_senco24_pair_write_database_sidecar",
        "created_at": conclusion_rows[0].get("finished_at") if conclusion_rows else datetime.now().isoformat(),
        "no_write": False,
        "opens_com_ports": True,
        "controls_water_or_gas_routes": False,
        "writes_device_id": False,
        "writes_senco2": True,
        "writes_senco4": True,
        "h2o_senco24_algorithm": h2o_senco24_algorithm,
        "clears_senco": False,
        "database_target_tables": ["sample_files", "coefficient_write_events", "audit_events"],
        "artifacts": artifacts,
        "suggested_rows": suggested_rows,
    }
    target = destination / "h2o_senco24_pair_write_database_sidecar.json"
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_h2o_senco24_controlled_write")
    except ValueError as exc:
        base._log(str(exc))
        return 2
    if not args.enable_senco24_write or args.operator_confirmation != CONFIRMATION_TEXT:
        base._log("Refusing SENCO2/SENCO4 write: pass --enable-senco24-write and exact confirmation text.")
        return 2
    if not str(args.reviewer or "").strip() or not str(args.approver or "").strip():
        base._log("Refusing SENCO2/SENCO4 write: reviewer and approver are required.")
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        base._log("Refusing SENCO2/SENCO4 write: reviewer and approver must differ.")
        return 2

    cfg_path = Path(args.config).resolve()
    review_dir = Path(args.review_dir).resolve()
    snapshot_path = Path(args.old_component_snapshot_json).resolve()
    old_snapshot = _load_json(snapshot_path)
    supported = _supported_rows(
        review_dir=review_dir,
        old_snapshot=old_snapshot,
        allow_review_required=bool(args.allow_review_required_candidates),
        allow_separate_senco6_layer_review=bool(args.allow_separate_senco6_layer_review),
        h2o_senco24_algorithm=str(args.h2o_senco24_algorithm),
    )
    targets = _select_targets(supported, selected_device_ids=args.device_id, write_all_ready=bool(args.write_all_ready))
    if not targets:
        base._log("No ready H2O SENCO2/SENCO4 pair targets selected.")
        return 2

    cfg = base.load_config(cfg_path)
    analyzer_by_id = base._build_analyzer_map(cfg)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    old_snapshot_out: Dict[str, Any] = {}
    start_ts = datetime.now().isoformat(timespec="seconds")

    for target_index, row in enumerate(targets):
        device_id = base._device_id(row.get("analyzer_device_id"))
        analyzer_cfg = analyzer_by_id.get(device_id)
        if analyzer_cfg is None:
            summary_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "analyzer_prefix": row.get("analyzer_prefix", ""),
                    "status": "skipped",
                    "reason": "device_id_not_found_in_config",
                    "write_applied": False,
                }
            )
            if args.stop_on_failure:
                break
            continue

        old_primary_review = [float(value) for value in row.get("_old_primary_values", [])]
        old_secondary_review = [float(value) for value in row.get("_old_secondary_values", [])]
        old_senco6_review = [float(value) for value in row.get("_old_senco6_values", [])]
        primary_target = [float(value) for value in row.get("_primary_target_values", [])]
        secondary_target = [float(value) for value in row.get("_secondary_target_values", [])]

        ga = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        identity_before: Dict[str, Any] = {}
        identity_after: Dict[str, Any] = {}
        restore: Dict[str, Any] = {}
        write_result: Dict[str, Any] = {}
        old_primary_live: List[float] = []
        old_secondary_live: List[float] = []
        old_senco6_live: List[float] = []
        status = "failed"
        reason = ""

        try:
            base._log(f"H2O SENCO2/SENCO4 controlled pair write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
            ga.open()
            base._configure_coefficient_io(ga, args)
            base._sleep_gap(float(args.pre_device_cooldown_s))
            identity_before = base._read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if base._device_id(identity_before.get("id")) != device_id:
                raise RuntimeError(
                    f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}"
                )
            old_primary_live = base._read_reviewed_group_values_with_retry(
                ga,
                2,
                old_primary_review,
                min_count=_review_read_min_count(old_primary_review),
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=float(args.compare_atol),
            )
            old_secondary_live = base._read_reviewed_group_values_with_retry(
                ga,
                4,
                old_secondary_review,
                min_count=_review_read_min_count(old_secondary_review),
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=float(args.compare_atol),
            )
            old_senco6_live = base._read_reviewed_group_values_with_retry(
                ga,
                6,
                old_senco6_review,
                min_count=_review_read_min_count(old_senco6_review),
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=float(args.compare_atol),
            )
            if not _neutral_senco6(old_senco6_live) and not bool(args.allow_separate_senco6_layer_review):
                raise RuntimeError("live_GETCO6_not_neutral_before_H2O_main_chain_write")

            write_result = _set_senco24_with_rollback(
                ga,
                primary_target_values=primary_target,
                secondary_target_values=secondary_target,
                old_primary_live=old_primary_live,
                old_secondary_live=old_secondary_live,
                restore_mode=int(analyzer_cfg.get("mode", 2) or 2),
                readback_attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                post_write_settle_s=max(0.0, float(args.post_write_settle_s)),
                compare_atol=float(args.compare_atol),
                write_attempts=max(1, int(args.write_attempts)),
            )
            restore = base._restore_analyzer_runtime(
                ga,
                analyzer_cfg,
                command_gap_s=float(args.restore_command_gap_s),
                restore_active_freq=bool(args.restore_active_freq),
            )
            base._sleep_gap(0.5)
            identity_after = base._read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if base._device_id(identity_after.get("id")) != device_id:
                raise RuntimeError(
                    f"post_write_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                )
            if bool(write_result.get("ok")):
                status = "written_readback_verified"
                reason = ""
            else:
                reason = str(write_result.get("failure_reason") or "paired_write_verification_failed")
        except Exception as exc:
            reason = str(exc)
            try:
                restore = base._restore_analyzer_runtime(
                    ga,
                    analyzer_cfg,
                    command_gap_s=float(args.restore_command_gap_s),
                    restore_active_freq=bool(args.restore_active_freq),
                )
            except Exception:
                pass
        finally:
            try:
                ga.close()
            except Exception:
                pass

        old_snapshot_out[device_id] = {
            "analyzer_prefix": row.get("analyzer_prefix", ""),
            "port": analyzer_cfg.get("port", ""),
            "GETCO2_before_review": old_primary_review,
            "GETCO2_before_live": old_primary_live,
            "GETCO4_before_review": old_secondary_review,
            "GETCO4_before_live": old_secondary_live,
            "GETCO6_before_review": old_senco6_review,
            "GETCO6_before_live": old_senco6_live,
            "candidate_senco2_values": primary_target,
            "candidate_senco4_values": secondary_target,
            "h2o_senco24_algorithm": str(args.h2o_senco24_algorithm),
            "senco2_readback": list(write_result.get("final_senco2_readback") or []),
            "senco4_readback": list(write_result.get("final_senco4_readback") or []),
        }
        summary_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "candidate_senco2_values": json.dumps(primary_target, ensure_ascii=False),
                "candidate_senco4_values": json.dumps(secondary_target, ensure_ascii=False),
                "h2o_senco24_algorithm": str(args.h2o_senco24_algorithm),
                "senco2_readback": json.dumps(write_result.get("final_senco2_readback") or [], ensure_ascii=False),
                "senco4_readback": json.dumps(write_result.get("final_senco4_readback") or [], ensure_ascii=False),
                "GETCO6_before_live": json.dumps(old_senco6_live, ensure_ascii=False),
                "senco6_separate_layer_reviewed": bool(row.get("_senco6_separate_layer_reviewed", False)),
                "manual_review_required_override": bool(row.get("_manual_review_required_override", False)),
                "status": status,
                "reason": reason,
                "write_applied": status == "written_readback_verified",
                "rollback_attempted": bool(write_result.get("rollback_attempted", False)),
                "rollback_senco2_confirmed": bool(write_result.get("rollback_senco2_confirmed", False)),
                "rollback_senco4_confirmed": bool(write_result.get("rollback_senco4_confirmed", False)),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "controls_water_or_gas_routes": False,
                "writes_device_id": False,
                "writes_senco2": status == "written_readback_verified",
                "writes_senco4": status == "written_readback_verified",
                "writes_senco6": False,
                "clears_senco": False,
                "reviewer": str(args.reviewer),
                "approver": str(args.approver),
            }
        )
        detail_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "identity_before_json": json.dumps(identity_before, ensure_ascii=False, default=str),
                "identity_after_json": json.dumps(identity_after, ensure_ascii=False, default=str),
                "old_senco2_review_json": json.dumps(old_primary_review, ensure_ascii=False),
                "old_senco2_live_json": json.dumps(old_primary_live, ensure_ascii=False),
                "old_senco4_review_json": json.dumps(old_secondary_review, ensure_ascii=False),
                "old_senco4_live_json": json.dumps(old_secondary_live, ensure_ascii=False),
                "old_senco6_live_json": json.dumps(old_senco6_live, ensure_ascii=False),
                "candidate_senco2_json": json.dumps(primary_target, ensure_ascii=False),
                "candidate_senco4_json": json.dumps(secondary_target, ensure_ascii=False),
                "h2o_senco24_algorithm": str(args.h2o_senco24_algorithm),
                "write_result_json": json.dumps(write_result, ensure_ascii=False, default=str),
                "runtime_restore_json": json.dumps(restore, ensure_ascii=False, default=str),
                "candidate_source_row_json": json.dumps(
                    {key: value for key, value in row.items() if not str(key).startswith("_")},
                    ensure_ascii=False,
                    default=str,
                ),
            }
        )
        if status != "written_readback_verified" and args.stop_on_failure:
            break
        if target_index + 1 < len(targets):
            base._sleep_gap(float(args.inter_device_delay_s))

    end_ts = datetime.now().isoformat(timespec="seconds")
    any_failed = any(str(row.get("status") or "") != "written_readback_verified" for row in summary_rows)
    conclusion_rows = [
        {
            "overall_status": "failed" if any_failed else "success",
            "started_at": start_ts,
            "finished_at": end_ts,
            "target_count": len(targets),
            "processed_count": len(summary_rows),
            "success_count": sum(1 for row in summary_rows if row.get("status") == "written_readback_verified"),
            "stop_on_failure": bool(args.stop_on_failure),
            "h2o_senco24_algorithm": str(args.h2o_senco24_algorithm),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco2": True,
            "writes_senco4": True,
            "writes_senco6": False,
            "clears_senco": False,
        }
    ]
    metadata = ValidationMetadata(
        tool_name="run_v1_5_h2o_senco24_controlled_write",
        created_at=end_ts,
        analyzers=[f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}" for row in summary_rows],
        input_paths=[str(cfg_path), str(review_dir), str(snapshot_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "write_all_ready": bool(args.write_all_ready),
            "device_ids": [base._device_id(item) for item in args.device_id],
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "h2o_senco24_algorithm": str(args.h2o_senco24_algorithm),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco2": True,
            "writes_senco4": True,
            "writes_senco6": False,
            "clears_senco": False,
            "pre_device_cooldown_s": float(args.pre_device_cooldown_s),
            "inter_device_delay_s": float(args.inter_device_delay_s),
            "restore_command_gap_s": float(args.restore_command_gap_s),
            "restore_active_freq": bool(args.restore_active_freq),
            "coefficient_quiet_settle_s": float(args.coefficient_quiet_settle_s),
            "coefficient_read_timeout_s": float(args.coefficient_read_timeout_s),
            "coefficient_read_delay_s": float(args.coefficient_read_delay_s),
            "coefficient_read_retries": int(args.coefficient_read_retries),
            "compare_atol": float(args.compare_atol),
            "write_attempts": int(args.write_attempts),
            "post_write_settle_s": max(0.0, float(args.post_write_settle_s)),
            "allow_review_required_candidates": bool(args.allow_review_required_candidates),
            "allow_separate_senco6_layer_review": bool(args.allow_separate_senco6_layer_review),
        },
        notes=[
            "Controlled real-device H2O SENCO2 + SENCO4 paired coefficient write.",
            (
                "New absorption contract: SENCO2 carries lnR0(T1) terms and SENCO4 carries k(T1) terms."
                if str(args.h2o_senco24_algorithm) == H2O_SENCO24_ALGORITHM_NEW_ABSORPTION
                else "Mature V1.5 contract: SENCO2 carries H2O ratio polynomial terms; SENCO4 carries T/T2/RT terms while pressure slots stay frozen at zero."
            ),
            "GETCO6 must be neutral unless SENCO6 has been explicitly reviewed as a separate final affine layer; this tool never writes or clears SENCO6.",
            "When the separate SENCO6 layer is enabled, the reviewed SENCO6 write must happen after the SENCO2/SENCO4 main-chain write.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, or CLEARSENCO commands are performed.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="h2o_senco24_pair_write",
        metadata=metadata,
        tables={
            "h2o_senco24_pair_write_summary": summary_rows,
            "h2o_senco24_pair_write_detail": detail_rows,
            "h2o_senco24_pair_write_conclusion": conclusion_rows,
        },
    )
    snapshot_out_path = destination / "old_getco2_getco4_getco6_snapshot.json"
    snapshot_out_path.write_text(json.dumps(old_snapshot_out, ensure_ascii=False, indent=2), encoding="utf-8")
    base._write_csv(
        destination / "old_getco2_getco4_getco6_snapshot.csv",
        [
            {
                "analyzer_device_id": device_id,
                "analyzer_prefix": payload.get("analyzer_prefix", ""),
                "port": payload.get("port", ""),
                "GETCO2_before_review": json.dumps(payload.get("GETCO2_before_review") or [], ensure_ascii=False),
                "GETCO2_before_live": json.dumps(payload.get("GETCO2_before_live") or [], ensure_ascii=False),
                "GETCO4_before_review": json.dumps(payload.get("GETCO4_before_review") or [], ensure_ascii=False),
                "GETCO4_before_live": json.dumps(payload.get("GETCO4_before_live") or [], ensure_ascii=False),
                "GETCO6_before_review": json.dumps(payload.get("GETCO6_before_review") or [], ensure_ascii=False),
                "GETCO6_before_live": json.dumps(payload.get("GETCO6_before_live") or [], ensure_ascii=False),
                "candidate_senco2_values": json.dumps(payload.get("candidate_senco2_values") or [], ensure_ascii=False),
                "candidate_senco4_values": json.dumps(payload.get("candidate_senco4_values") or [], ensure_ascii=False),
                "senco2_readback": json.dumps(payload.get("senco2_readback") or [], ensure_ascii=False),
                "senco4_readback": json.dumps(payload.get("senco4_readback") or [], ensure_ascii=False),
            }
            for device_id, payload in old_snapshot_out.items()
        ],
    )
    database_sidecar_path = _write_database_sidecar(
        destination,
        outputs=outputs,
        snapshot_path=snapshot_out_path,
        summary_rows=summary_rows,
        conclusion_rows=conclusion_rows,
        h2o_senco24_algorithm=str(args.h2o_senco24_algorithm),
    )
    base._log(f"Controlled H2O SENCO2/SENCO4 pair write report saved: {outputs['workbook']}")
    base._log(f"Old GETCO2/GETCO4/GETCO6 snapshot saved: {snapshot_out_path}")
    base._log(f"Database sidecar saved: {database_sidecar_path}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
