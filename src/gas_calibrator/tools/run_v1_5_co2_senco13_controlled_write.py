"""Run controlled V1.5 CO2 SENCO1 + SENCO3 paired writes.

This high-risk real-device tool writes only the reviewed CO2 coefficient pair:
SENCO1 for ratio polynomial terms and SENCO3 for temperature terms. It never
changes analyzer IDs, never clears coefficient groups, and never controls PACE,
valves, water routes, gas routes, or humidity generation.
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
from ..validation.v1_5_final_senco_prewrite_gate import validate_final_senco_prewrite_gate
from . import run_v1_5_co2_senco1_controlled_write as base
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO1_SENCO3_V1_5_CO2_PAIR"
FORMULA_CONTRACT_CHECK = "firmware_formula_contract_confirmed"
SENCO5_CONTRACT_CHECK = "co2_senco5_senco6_linear_correction_contract"
SENCO5_CONTRACT_CHECK_LEGACY = "co2_density_temperature_senco5_contract"
PAIR_WRITE_PAYLOAD_WIDTH = 6
SECONDARY_PRESSURE_SLOT_START = 3
SECONDARY_PRESSURE_SLOT_ZERO_ATOL = 1e-12


def _secondary_pressure_target_slots_zero(values: Sequence[float], *, atol: float = SECONDARY_PRESSURE_SLOT_ZERO_ATOL) -> bool:
    """True only when SENCO3/SENCO4 pressure target slots are frozen at zero."""

    if len(values) != PAIR_WRITE_PAYLOAD_WIDTH:
        return False
    try:
        return all(abs(float(value)) <= float(atol) for value in values[SECONDARY_PRESSURE_SLOT_START:])
    except Exception:
        return False


def _formula_contract_review_passed(review_dir: Path) -> tuple[bool, str]:
    checks_path = Path(review_dir) / "candidate_write_review_checks.csv"
    if not checks_path.exists():
        return False, f"{FORMULA_CONTRACT_CHECK}_check_missing"
    checks = {
        str(row.get("check") or "").strip(): str(row.get("status") or "").strip().lower()
        for row in base._read_csv(checks_path)
    }
    for check_name in (FORMULA_CONTRACT_CHECK, SENCO5_CONTRACT_CHECK):
        status = checks.get(check_name)
        if check_name == SENCO5_CONTRACT_CHECK and status is None:
            status = checks.get(SENCO5_CONTRACT_CHECK_LEGACY)
        if status != "pass":
            return False, f"{check_name}:{status or 'check_missing'}"
    return True, ""


def _supported_pair_rows(mapping_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in mapping_rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        if str(row.get("primary_senco") or "").strip().upper() != "SENCO1":
            continue
        if str(row.get("secondary_senco") or "").strip().upper() != "SENCO3":
            continue
        if not base._truthy(row.get("candidate_terms_complete")):
            continue
        if not base._truthy(row.get("secondary_candidate_terms_complete")):
            continue
        if str(row.get("old_snapshot_status") or "").strip() != "primary_and_secondary_bound":
            continue
        if str(row.get("mapping_status") or "").strip() != "review_only_primary_secondary_preview_ready":
            continue

        primary_target = base._parse_values(row.get("primary_candidate_values"))
        secondary_target = base._parse_values(row.get("secondary_candidate_values"))
        old_primary = base._parse_values(row.get("old_primary_snapshot"))
        old_secondary = base._parse_values(row.get("old_secondary_snapshot"))
        if len(primary_target) != PAIR_WRITE_PAYLOAD_WIDTH or len(secondary_target) != PAIR_WRITE_PAYLOAD_WIDTH:
            continue
        if not _secondary_pressure_target_slots_zero(secondary_target):
            continue
        if len(old_primary) != PAIR_WRITE_PAYLOAD_WIDTH or len(old_secondary) != PAIR_WRITE_PAYLOAD_WIDTH:
            continue

        item = dict(row)
        item["_primary_target_values"] = primary_target
        item["_secondary_target_values"] = secondary_target
        item["_old_primary_values"] = old_primary
        item["_old_secondary_values"] = old_secondary
        out.append(item)
    return out


def _select_targets(
    mapping_rows: Sequence[Mapping[str, Any]],
    *,
    selected_device_ids: Sequence[str],
    write_all_ready: bool,
) -> List[Dict[str, Any]]:
    supported = _supported_pair_rows(mapping_rows)
    if write_all_ready:
        return supported
    wanted = {base._device_id(item) for item in selected_device_ids if str(item or "").strip()}
    return [row for row in supported if base._device_id(row.get("analyzer_device_id")) in wanted]


def _write_group_with_readback(
    ga: GasAnalyzer,
    group: int,
    target_values: Sequence[float],
    *,
    readback_attempts: int,
    retry_delay_s: float,
    post_write_settle_s: float,
    compare_atol: float,
    write_attempts: int,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "group": int(group),
        "target": list(target_values),
        "readback": [],
        "write_status": "pending",
        "verify_status": "pending",
        "failure_reason": "",
    }
    last_readback: List[float] = []
    last_error = ""
    for write_idx in range(max(1, int(write_attempts))):
        acked = bool(ga.set_senco(int(group), *target_values))
        result["write_status"] = "success" if acked else "ack_missing_readback_check"
        last_error = "" if acked else f"SENCO{int(group)}_WRITE_ACK_FAILED"
        base._sleep_gap(post_write_settle_s)
        for idx in range(max(1, int(readback_attempts))):
            try:
                last_readback = base._read_group_values(ga, int(group), min_count=len(list(target_values)))
                if senco_readback_matches(
                    target_values,
                    last_readback[: len(list(target_values))],
                    atol=float(compare_atol),
                ):
                    result["readback"] = list(last_readback)
                    result["verify_status"] = "success"
                    return result
                last_error = f"SENCO{int(group)}_READBACK_MISMATCH"
            except Exception as exc:
                last_error = str(exc)
            if idx + 1 < max(1, int(readback_attempts)):
                base._sleep_gap(retry_delay_s)
        if write_idx + 1 < max(1, int(write_attempts)):
            base._sleep_gap(retry_delay_s)

    result["readback"] = list(last_readback)
    result["failure_reason"] = last_error or f"SENCO{int(group)}_READBACK_MISSING"
    return result


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
            min_count=len(list(reviewed)),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=float(retry_delay_s),
            atol=float(compare_atol),
        )
    except Exception as exc:
        return False, [], str(exc)
    return True, list(values), ""


def _set_senco13_with_rollback(
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
        "old_primary_live": list(old_primary_live),
        "old_secondary_live": list(old_secondary_live),
        "target_senco1": list(primary_target_values),
        "target_senco3": list(secondary_target_values),
        "senco1_result": {},
        "senco3_result": {},
        "final_senco1_readback": [],
        "final_senco3_readback": [],
        "rollback_attempted": False,
        "rollback_senco1_confirmed": False,
        "rollback_senco3_confirmed": False,
        "failure_reason": "",
    }
    primary_attempted = False
    secondary_attempted = False
    try:
        if not ga.set_mode_with_ack(2, require_ack=True):
            raise RuntimeError("MODE=2 not acknowledged before SENCO1/SENCO3 write")
        base._sleep_gap(retry_delay_s)

        primary = _write_group_with_readback(
            ga,
            1,
            primary_target_values,
            readback_attempts=readback_attempts,
            retry_delay_s=retry_delay_s,
            post_write_settle_s=post_write_settle_s,
            compare_atol=compare_atol,
            write_attempts=write_attempts,
        )
        primary_attempted = True
        result["senco1_result"] = primary
        if primary.get("verify_status") != "success":
            raise RuntimeError(primary.get("failure_reason") or "SENCO1_VERIFY_FAILED")

        secondary = _write_group_with_readback(
            ga,
            3,
            secondary_target_values,
            readback_attempts=readback_attempts,
            retry_delay_s=retry_delay_s,
            post_write_settle_s=post_write_settle_s,
            compare_atol=compare_atol,
            write_attempts=write_attempts,
        )
        secondary_attempted = True
        result["senco3_result"] = secondary
        if secondary.get("verify_status") != "success":
            raise RuntimeError(secondary.get("failure_reason") or "SENCO3_VERIFY_FAILED")

        result["final_senco1_readback"] = base._read_group_values_with_retry(
            ga,
            1,
            min_count=len(list(primary_target_values)),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=retry_delay_s,
        )
        result["final_senco3_readback"] = base._read_group_values_with_retry(
            ga,
            3,
            min_count=len(list(secondary_target_values)),
            attempts=max(1, int(readback_attempts)),
            retry_delay_s=retry_delay_s,
        )
        if not senco_readback_matches(
            primary_target_values,
            result["final_senco1_readback"][: len(list(primary_target_values))],
            atol=float(compare_atol),
        ):
            raise RuntimeError("SENCO1_FINAL_READBACK_MISMATCH")
        if not senco_readback_matches(
            secondary_target_values,
            result["final_senco3_readback"][: len(list(secondary_target_values))],
            atol=float(compare_atol),
        ):
            raise RuntimeError("SENCO3_FINAL_READBACK_MISMATCH")
        result["ok"] = True
    except Exception as exc:
        result["failure_reason"] = str(exc)
        if primary_attempted or secondary_attempted:
            result["rollback_attempted"] = True
            try:
                ga.set_senco(3, *old_secondary_live)
                base._sleep_gap(post_write_settle_s)
                ok3, rb3, reason3 = _verify_review_snapshot(
                    ga,
                    3,
                    old_secondary_live,
                    readback_attempts=readback_attempts,
                    retry_delay_s=retry_delay_s,
                    compare_atol=compare_atol,
                )
                result["rollback_senco3_readback"] = rb3
                result["rollback_senco3_confirmed"] = bool(ok3)
                if not ok3:
                    result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO3_FAILED:{reason3}"
            except Exception as rollback_exc:
                result["rollback_senco3_confirmed"] = False
                result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO3_FAILED:{rollback_exc}"
            try:
                ga.set_senco(1, *old_primary_live)
                base._sleep_gap(post_write_settle_s)
                ok1, rb1, reason1 = _verify_review_snapshot(
                    ga,
                    1,
                    old_primary_live,
                    readback_attempts=readback_attempts,
                    retry_delay_s=retry_delay_s,
                    compare_atol=compare_atol,
                )
                result["rollback_senco1_readback"] = rb1
                result["rollback_senco1_confirmed"] = bool(ok1)
                if not ok1:
                    result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO1_FAILED:{reason1}"
            except Exception as rollback_exc:
                result["rollback_senco1_confirmed"] = False
                result["failure_reason"] = f"{result['failure_reason']}; ROLLBACK_SENCO1_FAILED:{rollback_exc}"
    finally:
        try:
            ga.set_mode_with_ack(int(restore_mode), require_ack=False)
        except Exception:
            pass
    return result


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 CO2 SENCO1 + SENCO3 paired coefficient writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--review-dir", required=True, help="Directory containing candidate_senco_mapping_review.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to write.")
    parser.add_argument("--write-all-ready", action="store_true", help="Write every ready paired CO2 candidate.")
    parser.add_argument("--enable-senco13-write", action="store_true", help="Required to write SENCO1 and SENCO3.")
    parser.add_argument("--operator-confirmation", default="", help=f"Must equal {CONFIRMATION_TEXT!r}.")
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--stop-on-failure", action="store_true", default=True)
    parser.add_argument("--continue-on-failure", dest="stop_on_failure", action="store_false")
    parser.add_argument("--identity-timeout-s", type=float, default=4.0)
    parser.add_argument("--readback-attempts", type=int, default=3)
    parser.add_argument("--write-attempts", type=int, default=2)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument(
        "--post-write-settle-s",
        type=float,
        default=1.0,
        help="Delay after each SENCO write before GETCO readback; protects fragile analyzer serial/flash handling.",
    )
    parser.add_argument("--compare-atol", type=float, default=1e-9)
    parser.add_argument("--pre-device-cooldown-s", type=float, default=2.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=5.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument("--restore-active-freq", action="store_true", default=True)
    parser.add_argument("--no-restore-active-freq", dest="restore_active_freq", action="store_false")
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=1.5)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=2)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_database_sidecar(
    destination: Path,
    *,
    outputs: Mapping[str, Path],
    snapshot_path: Path,
    summary_rows: Sequence[Mapping[str, Any]],
    conclusion_rows: Sequence[Mapping[str, Any]],
) -> Path:
    artifacts: List[Dict[str, Any]] = []
    artifact_paths: Dict[str, Path] = {key: Path(value) for key, value in outputs.items()}
    artifact_paths["old_getco1_getco3_snapshot_json"] = snapshot_path
    csv_snapshot = snapshot_path.with_suffix(".csv")
    if csv_snapshot.exists():
        artifact_paths["old_getco1_getco3_snapshot_csv"] = csv_snapshot

    for key, path in sorted(artifact_paths.items()):
        if not path.exists():
            continue
        role = "coefficient_snapshot" if "snapshot" in key else "coefficient_write_log"
        artifacts.append(
            {
                "output_key": key,
                "artifact_role": role,
                "path": str(path.resolve()),
                "sha256": _sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )

    old_coefficients_hash = _sha256_file(snapshot_path) if snapshot_path.exists() else ""
    suggested_rows: List[Dict[str, Any]] = []
    for row in summary_rows:
        device_id = base._device_id(row.get("analyzer_device_id"))
        readback = {
            "senco1": base._parse_values(row.get("senco1_readback")),
            "senco3": base._parse_values(row.get("senco3_readback")),
            "identity_before": row.get("identity_before", ""),
            "identity_after": row.get("identity_after", ""),
        }
        suggested_rows.append(
            {
                "db_table": "coefficient_write_events",
                "record_key": f"co2_senco13_pair_write_{device_id}",
                "component": "co2",
                "analyzer_device_id": device_id,
                "event_type": "co2_senco1_senco3_paired_write",
                "status": str(row.get("status") or "unknown"),
                "approved_by": row.get("approver"),
                "candidate_id": "co2_senco1_senco3_pair_review",
                "old_coefficients_hash": old_coefficients_hash,
                "command_summary": (
                    "SENCO1 and SENCO3 paired CO2 coefficient write; P/RTP target slots remain frozen at zero; "
                    "no route control, no device-ID write, no CLEARSENCO."
                ),
                "readback_json": json.dumps(readback, ensure_ascii=False),
                "source_artifact_role": "coefficient_write_log",
                "source_path": str(destination.resolve()),
                "metadata_json": json.dumps(
                    {
                        "reviewer": row.get("reviewer", ""),
                        "approver": row.get("approver", ""),
                        "write_applied": base._truthy(row.get("write_applied")),
                        "rollback_attempted": base._truthy(row.get("rollback_attempted")),
                        "runtime_restore_status": row.get("runtime_restore_status", ""),
                        "active_freq_restore_status": row.get("active_freq_restore_status", ""),
                        "controls_water_or_gas_routes": False,
                        "writes_device_id": False,
                        "writes_senco1": base._truthy(row.get("writes_senco1")),
                        "writes_senco3": base._truthy(row.get("writes_senco3")),
                        "clears_senco": False,
                    },
                    ensure_ascii=False,
                ),
            }
        )

    payload = {
        "schema": "v1_5_co2_senco13_pair_write_database_sidecar",
        "created_at": conclusion_rows[0].get("finished_at") if conclusion_rows else datetime.now().isoformat(),
        "no_write": False,
        "opens_com_ports": True,
        "controls_water_or_gas_routes": False,
        "writes_device_id": False,
        "writes_senco1": True,
        "writes_senco3": True,
        "clears_senco": False,
        "database_target_tables": ["sample_files", "coefficient_write_events", "audit_events"],
        "artifacts": artifacts,
        "suggested_rows": suggested_rows,
    }
    target = destination / "co2_senco13_pair_write_database_sidecar.json"
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_co2_senco13_controlled_write")
    except ValueError as exc:
        base._log(str(exc))
        return 2
    if not args.enable_senco13_write or args.operator_confirmation != CONFIRMATION_TEXT:
        base._log("Refusing SENCO1/SENCO3 write: pass --enable-senco13-write and exact confirmation text.")
        return 2
    if not str(args.reviewer or "").strip() or not str(args.approver or "").strip():
        base._log("Refusing SENCO1/SENCO3 write: reviewer and approver are required.")
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        base._log("Refusing SENCO1/SENCO3 write: reviewer and approver must differ.")
        return 2

    cfg_path = Path(args.config).resolve()
    review_dir = Path(args.review_dir).resolve()
    mapping_path = review_dir / "candidate_senco_mapping_review.csv"
    if not mapping_path.exists():
        base._log(f"Candidate mapping review not found: {mapping_path}")
        return 2
    formula_ok, formula_reason = _formula_contract_review_passed(review_dir)
    if not formula_ok:
        base._log(f"Refusing SENCO1/SENCO3 write: formula contract not confirmed ({formula_reason}).")
        return 2

    cfg = base.load_config(cfg_path)
    mapping_rows = base._read_csv(mapping_path)
    targets = _select_targets(mapping_rows, selected_device_ids=args.device_id, write_all_ready=bool(args.write_all_ready))
    if not targets:
        base._log("No ready CO2 SENCO1/SENCO3 pair targets selected.")
        return 2
    prewrite_ok, prewrite_reasons, prewrite_detail = validate_final_senco_prewrite_gate(
        review_dir,
        component="co2",
        device_ids=[base._device_id(row.get("analyzer_device_id")) for row in targets],
    )
    if not prewrite_ok:
        base._log(
            "Refusing SENCO1/SENCO3 write: final fit-input prewrite gate failed "
            f"({';'.join(prewrite_reasons)})."
        )
        return 2

    analyzer_by_id = base._build_analyzer_map(cfg)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    old_snapshot: Dict[str, Any] = {}
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
        primary_candidate = [float(value) for value in row.get("_primary_target_values", [])]
        secondary_candidate = [float(value) for value in row.get("_secondary_target_values", [])]
        primary_target = [float(value) for value in primary_candidate]
        secondary_target = [float(value) for value in secondary_candidate]

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
        status = "failed"
        reason = ""

        try:
            base._log(f"CO2 SENCO1/SENCO3 controlled pair write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
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
                1,
                old_primary_review,
                min_count=len(old_primary_review),
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=float(args.compare_atol),
            )
            old_secondary_live = base._read_reviewed_group_values_with_retry(
                ga,
                3,
                old_secondary_review,
                min_count=len(old_secondary_review),
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
                atol=float(args.compare_atol),
            )

            write_result = _set_senco13_with_rollback(
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

        old_snapshot[device_id] = {
            "analyzer_prefix": row.get("analyzer_prefix", ""),
            "port": analyzer_cfg.get("port", ""),
            "GETCO1_before_review": old_primary_review,
            "GETCO1_before_live": old_primary_live,
            "GETCO3_before_review": old_secondary_review,
            "GETCO3_before_live": old_secondary_live,
            "candidate_senco1_values": primary_candidate,
            "target_senco1_payload_values": primary_target,
            "candidate_senco3_values": secondary_candidate,
            "target_senco3_payload_values": secondary_target,
            "senco1_readback": list(write_result.get("final_senco1_readback") or []),
            "senco3_readback": list(write_result.get("final_senco3_readback") or []),
        }
        summary_rows.append(
            {
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "candidate_senco1_values": json.dumps(primary_candidate, ensure_ascii=False),
                "target_senco1_values": json.dumps(primary_target, ensure_ascii=False),
                "candidate_senco3_values": json.dumps(secondary_candidate, ensure_ascii=False),
                "target_senco3_values": json.dumps(secondary_target, ensure_ascii=False),
                "senco1_readback": json.dumps(write_result.get("final_senco1_readback") or [], ensure_ascii=False),
                "senco3_readback": json.dumps(write_result.get("final_senco3_readback") or [], ensure_ascii=False),
                "status": status,
                "reason": reason,
                "write_applied": status == "written_readback_verified",
                "rollback_attempted": bool(write_result.get("rollback_attempted", False)),
                "rollback_senco1_confirmed": bool(write_result.get("rollback_senco1_confirmed", False)),
                "rollback_senco3_confirmed": bool(write_result.get("rollback_senco3_confirmed", False)),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "controls_water_or_gas_routes": False,
                "writes_device_id": False,
                "writes_senco1": status == "written_readback_verified",
                "writes_senco3": status == "written_readback_verified",
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
                "old_primary_review_json": json.dumps(old_primary_review, ensure_ascii=False),
                "old_primary_live_json": json.dumps(old_primary_live, ensure_ascii=False),
                "old_secondary_review_json": json.dumps(old_secondary_review, ensure_ascii=False),
                "old_secondary_live_json": json.dumps(old_secondary_live, ensure_ascii=False),
                "candidate_senco1_json": json.dumps(primary_candidate, ensure_ascii=False),
                "target_senco1_json": json.dumps(primary_target, ensure_ascii=False),
                "candidate_senco3_json": json.dumps(secondary_candidate, ensure_ascii=False),
                "target_senco3_json": json.dumps(secondary_target, ensure_ascii=False),
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
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco1": True,
            "writes_senco3": True,
            "artifact_hash_status": str(prewrite_detail.get("artifact_hash_status") or "blocked"),
            "artifact_hash_count": int(prewrite_detail.get("artifact_hash_count") or 0),
            "clears_senco": False,
        }
    ]
    metadata = ValidationMetadata(
        tool_name="run_v1_5_co2_senco13_controlled_write",
        created_at=end_ts,
        analyzers=[f"{row.get('analyzer_prefix')}:{row.get('analyzer_device_id')}" for row in summary_rows],
        input_paths=[
            str(cfg_path),
            str(mapping_path),
            str(prewrite_detail.get("meta_path") or ""),
            str(prewrite_detail.get("hash_manifest_path") or ""),
        ],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "write_all_ready": bool(args.write_all_ready),
            "device_ids": [base._device_id(item) for item in args.device_id],
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco1": True,
            "writes_senco3": True,
            "fit_input_traceability_status": str(prewrite_detail.get("fit_input_traceability_status") or "blocked"),
            "artifact_hash_status": str(prewrite_detail.get("artifact_hash_status") or "blocked"),
            "artifact_hash_count": int(prewrite_detail.get("artifact_hash_count") or 0),
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
        },
        notes=[
            "Controlled real-device CO2 SENCO1 + SENCO3 paired coefficient write.",
            "SENCO1 carries ratio polynomial terms; SENCO3 carries T/T2/RT terms while P/RTP target slots stay frozen at zero.",
            "A post-SENCO-write settle delay is enforced before GETCO readback to avoid overdriving fragile analyzer serial/flash handling.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, or CLEARSENCO commands are performed.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="co2_senco13_pair_write",
        metadata=metadata,
        tables={
            "co2_senco13_pair_write_summary": summary_rows,
            "co2_senco13_pair_write_detail": detail_rows,
            "co2_senco13_pair_write_conclusion": conclusion_rows,
        },
    )
    snapshot_path = destination / "old_getco1_getco3_snapshot.json"
    snapshot_path.write_text(json.dumps(old_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    base._write_csv(
        destination / "old_getco1_getco3_snapshot.csv",
        [
            {
                "analyzer_device_id": device_id,
                "analyzer_prefix": payload.get("analyzer_prefix", ""),
                "port": payload.get("port", ""),
                "GETCO1_before_review": json.dumps(payload.get("GETCO1_before_review") or [], ensure_ascii=False),
                "GETCO1_before_live": json.dumps(payload.get("GETCO1_before_live") or [], ensure_ascii=False),
                "GETCO3_before_review": json.dumps(payload.get("GETCO3_before_review") or [], ensure_ascii=False),
                "GETCO3_before_live": json.dumps(payload.get("GETCO3_before_live") or [], ensure_ascii=False),
                "candidate_senco1_values": json.dumps(payload.get("candidate_senco1_values") or [], ensure_ascii=False),
                "target_senco1_payload_values": json.dumps(
                    payload.get("target_senco1_payload_values") or [], ensure_ascii=False
                ),
                "candidate_senco3_values": json.dumps(payload.get("candidate_senco3_values") or [], ensure_ascii=False),
                "target_senco3_payload_values": json.dumps(
                    payload.get("target_senco3_payload_values") or [], ensure_ascii=False
                ),
                "senco1_readback": json.dumps(payload.get("senco1_readback") or [], ensure_ascii=False),
                "senco3_readback": json.dumps(payload.get("senco3_readback") or [], ensure_ascii=False),
            }
            for device_id, payload in old_snapshot.items()
        ],
    )
    database_sidecar_path = _write_database_sidecar(
        destination,
        outputs=outputs,
        snapshot_path=snapshot_path,
        summary_rows=summary_rows,
        conclusion_rows=conclusion_rows,
    )
    base._log(f"Controlled CO2 SENCO1/SENCO3 pair write report saved: {outputs['workbook']}")
    base._log(f"Old GETCO1/GETCO3 snapshot saved: {snapshot_path}")
    base._log(f"Database sidecar saved: {database_sidecar_path}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
