"""Rollback V1.5 CO2 SENCO1 + SENCO3 from a controlled-write snapshot.

This tool is intentionally narrow: it writes only the old GETCO1/GETCO3 values
captured before a paired CO2 write. It never changes analyzer IDs, never clears
coefficients, and never controls PACE, valves, water routes, gas routes, or
humidity generation.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..devices import GasAnalyzer
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_co2_senco1_controlled_write as base
from . import run_v1_5_co2_senco13_controlled_write as pair_writer
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "ROLLBACK_SENCO1_SENCO3_V1_5_CO2_PAIR"


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 CO2 SENCO1/SENCO3 rollback.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--write-dir", required=True, help="Directory containing old_getco1_getco3_snapshot.json.")
    parser.add_argument("--output-dir", required=True, help="Output directory for rollback evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to rollback.")
    parser.add_argument("--enable-senco13-rollback", action="store_true", help="Required to write old SENCO1/SENCO3.")
    parser.add_argument("--operator-confirmation", default="", help=f"Must equal {CONFIRMATION_TEXT!r}.")
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--identity-timeout-s", type=float, default=4.0)
    parser.add_argument("--readback-attempts", type=int, default=3)
    parser.add_argument("--write-attempts", type=int, default=2)
    parser.add_argument("--readback-retry-delay-s", type=float, default=1.0)
    parser.add_argument(
        "--post-write-settle-s",
        type=float,
        default=1.0,
        help="Delay after each rollback SENCO write before GETCO readback.",
    )
    parser.add_argument("--compare-atol", type=float, default=1e-9)
    parser.add_argument("--pre-device-cooldown-s", type=float, default=3.0)
    parser.add_argument("--inter-device-delay-s", type=float, default=6.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    parser.add_argument("--restore-active-freq", action="store_true", default=True)
    parser.add_argument("--no-restore-active-freq", dest="restore_active_freq", action="store_false")
    parser.add_argument("--coefficient-quiet-settle-s", type=float, default=3.0)
    parser.add_argument("--coefficient-read-timeout-s", type=float, default=2.0)
    parser.add_argument("--coefficient-read-delay-s", type=float, default=1.0)
    parser.add_argument("--coefficient-read-retries", type=int, default=3)
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_old_snapshot(path: Path) -> Dict[str, Mapping[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"old snapshot must be an object: {path}")
    return {base._device_id(key): value for key, value in payload.items() if isinstance(value, Mapping)}


def _old_values(payload: Mapping[str, Any], key: str) -> List[float]:
    values = payload.get(key)
    if not values:
        return []
    return [float(value) for value in values]


def _write_database_sidecar(
    destination: Path,
    *,
    write_dir: Path,
    snapshot_path: Path,
    outputs: Mapping[str, Path],
    summary_rows: List[Mapping[str, Any]],
    finished_at: str,
) -> Path:
    artifacts: List[Dict[str, Any]] = []
    artifact_paths = {key: Path(value) for key, value in outputs.items()}
    artifact_paths["source_old_getco1_getco3_snapshot_json"] = snapshot_path
    for key, path in sorted(artifact_paths.items()):
        if not path.exists():
            continue
        artifacts.append(
            {
                "output_key": key,
                "artifact_role": "coefficient_snapshot" if "snapshot" in key else "coefficient_write_log",
                "path": str(path.resolve()),
                "sha256": pair_writer._sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    snapshot_hash = pair_writer._sha256_file(snapshot_path) if snapshot_path.exists() else ""
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
                "record_key": f"co2_senco13_pair_rollback_{device_id}",
                "component": "co2",
                "analyzer_device_id": device_id,
                "event_type": "co2_senco1_senco3_pair_rollback",
                "status": str(row.get("status") or "unknown"),
                "approved_by": row.get("approver"),
                "candidate_id": "co2_senco1_senco3_pair_review",
                "old_coefficients_hash": snapshot_hash,
                "command_summary": (
                    "Rollback SENCO1 and SENCO3 to pre-write GETCO snapshot after post-write verification failure; "
                    "no route control, no device-ID write, no CLEARSENCO."
                ),
                "readback_json": json.dumps(readback, ensure_ascii=False),
                "source_artifact_role": "coefficient_write_log",
                "source_path": str(destination.resolve()),
                "metadata_json": json.dumps(
                    {
                        "source_write_dir": str(write_dir.resolve()),
                        "rollback_applied": base._truthy(row.get("rollback_applied")),
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
    target = destination / "co2_senco13_pair_rollback_database_sidecar.json"
    target.write_text(
        json.dumps(
            {
                "schema": "v1_5_co2_senco13_pair_rollback_database_sidecar",
                "created_at": finished_at,
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
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return target


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_co2_senco13_controlled_rollback")
    except ValueError as exc:
        base._log(str(exc))
        return 2
    if not args.enable_senco13_rollback or args.operator_confirmation != CONFIRMATION_TEXT:
        base._log("Refusing SENCO1/SENCO3 rollback: pass --enable-senco13-rollback and exact confirmation text.")
        return 2
    if not str(args.reviewer or "").strip() or not str(args.approver or "").strip():
        base._log("Refusing SENCO1/SENCO3 rollback: reviewer and approver are required.")
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        base._log("Refusing SENCO1/SENCO3 rollback: reviewer and approver must differ.")
        return 2

    cfg_path = Path(args.config).resolve()
    write_dir = Path(args.write_dir).resolve()
    snapshot_path = write_dir / "old_getco1_getco3_snapshot.json"
    if not snapshot_path.exists():
        base._log(f"Old GETCO1/GETCO3 snapshot not found: {snapshot_path}")
        return 2
    selected_ids = [base._device_id(item) for item in args.device_id if str(item or "").strip()]
    if not selected_ids:
        base._log("No rollback device IDs selected.")
        return 2

    cfg = base.load_config(cfg_path)
    analyzer_by_id = base._build_analyzer_map(cfg)
    old_snapshot = _load_old_snapshot(snapshot_path)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    start_ts = datetime.now().isoformat(timespec="seconds")

    for idx, device_id in enumerate(selected_ids):
        analyzer_cfg = analyzer_by_id.get(device_id)
        snapshot = old_snapshot.get(device_id)
        status = "failed"
        reason = ""
        restore: Dict[str, Any] = {}
        identity_before: Dict[str, Any] = {}
        identity_after: Dict[str, Any] = {}
        current_senco1: List[float] = []
        current_senco3: List[float] = []
        target_senco1: List[float] = []
        target_senco3: List[float] = []
        write_1: Dict[str, Any] = {}
        write_3: Dict[str, Any] = {}

        if analyzer_cfg is None:
            reason = "device_id_not_found_in_config"
        elif snapshot is None:
            reason = "old_snapshot_missing_for_device"
        else:
            target_senco1 = _old_values(snapshot, "GETCO1_before_live") or _old_values(snapshot, "GETCO1_before_review")
            target_senco3 = _old_values(snapshot, "GETCO3_before_live") or _old_values(snapshot, "GETCO3_before_review")
            if len(target_senco1) < 4 or len(target_senco3) < 4:
                reason = "old_snapshot_values_incomplete"
            else:
                ga = GasAnalyzer(
                    str(analyzer_cfg["port"]),
                    int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
                    timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
                    device_id=device_id,
                )
                try:
                    base._log(f"CO2 SENCO1/SENCO3 rollback begin: device_id={device_id} port={analyzer_cfg.get('port')}")
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
                    current_senco1 = base._read_group_values_with_retry(
                        ga,
                        1,
                        min_count=4,
                        attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                    )
                    current_senco3 = base._read_group_values_with_retry(
                        ga,
                        3,
                        min_count=4,
                        attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                    )
                    if not ga.set_mode_with_ack(2, require_ack=True):
                        raise RuntimeError("MODE=2 not acknowledged before rollback")
                    write_3 = pair_writer._write_group_with_readback(
                        ga,
                        3,
                        target_senco3,
                        readback_attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                        post_write_settle_s=max(0.0, float(args.post_write_settle_s)),
                        compare_atol=float(args.compare_atol),
                        write_attempts=max(1, int(args.write_attempts)),
                    )
                    if write_3.get("verify_status") != "success":
                        raise RuntimeError(write_3.get("failure_reason") or "SENCO3_ROLLBACK_VERIFY_FAILED")
                    write_1 = pair_writer._write_group_with_readback(
                        ga,
                        1,
                        target_senco1,
                        readback_attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                        post_write_settle_s=max(0.0, float(args.post_write_settle_s)),
                        compare_atol=float(args.compare_atol),
                        write_attempts=max(1, int(args.write_attempts)),
                    )
                    if write_1.get("verify_status") != "success":
                        raise RuntimeError(write_1.get("failure_reason") or "SENCO1_ROLLBACK_VERIFY_FAILED")
                    restore = base._restore_analyzer_runtime(
                        ga,
                        analyzer_cfg,
                        command_gap_s=float(args.restore_command_gap_s),
                        restore_active_freq=bool(args.restore_active_freq),
                    )
                    identity_after = base._read_identity_snapshot(
                        ga,
                        prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                        timeout_s=float(args.identity_timeout_s),
                    )
                    if base._device_id(identity_after.get("id")) != device_id:
                        raise RuntimeError(
                            f"post_rollback_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                        )
                    status = "rollback_readback_verified"
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

        summary_rows.append(
            {
                "analyzer_device_id": device_id,
                "analyzer_prefix": (analyzer_cfg or {}).get("name", "") if analyzer_cfg else "",
                "port": (analyzer_cfg or {}).get("port", "") if analyzer_cfg else "",
                "target_senco1_values": json.dumps(target_senco1, ensure_ascii=False),
                "target_senco3_values": json.dumps(target_senco3, ensure_ascii=False),
                "current_senco1_before_rollback": json.dumps(current_senco1, ensure_ascii=False),
                "current_senco3_before_rollback": json.dumps(current_senco3, ensure_ascii=False),
                "senco1_readback": json.dumps(write_1.get("readback") or [], ensure_ascii=False),
                "senco3_readback": json.dumps(write_3.get("readback") or [], ensure_ascii=False),
                "status": status,
                "reason": reason,
                "rollback_applied": status == "rollback_readback_verified",
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "controls_water_or_gas_routes": False,
                "writes_device_id": False,
                "writes_senco1": status == "rollback_readback_verified",
                "writes_senco3": status == "rollback_readback_verified",
                "clears_senco": False,
                "reviewer": str(args.reviewer),
                "approver": str(args.approver),
            }
        )
        detail_rows.append(
            {
                "analyzer_device_id": device_id,
                "identity_before_json": json.dumps(identity_before, ensure_ascii=False, default=str),
                "identity_after_json": json.dumps(identity_after, ensure_ascii=False, default=str),
                "write_senco1_json": json.dumps(write_1, ensure_ascii=False, default=str),
                "write_senco3_json": json.dumps(write_3, ensure_ascii=False, default=str),
                "runtime_restore_json": json.dumps(restore, ensure_ascii=False, default=str),
            }
        )
        if status != "rollback_readback_verified":
            break
        if idx + 1 < len(selected_ids):
            base._sleep_gap(float(args.inter_device_delay_s))

    end_ts = datetime.now().isoformat(timespec="seconds")
    failed = any(row.get("status") != "rollback_readback_verified" for row in summary_rows)
    metadata = ValidationMetadata(
        tool_name="run_v1_5_co2_senco13_controlled_rollback",
        created_at=end_ts,
        analyzers=[str(row.get("analyzer_device_id") or "") for row in summary_rows],
        input_paths=[str(cfg_path), str(snapshot_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "device_ids": selected_ids,
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "controls_water_or_gas_routes": False,
            "writes_device_id": False,
            "writes_senco1": True,
            "writes_senco3": True,
            "clears_senco": False,
            "post_write_settle_s": max(0.0, float(args.post_write_settle_s)),
        },
        notes=[
            "Controlled rollback after post-write verification failed on selected devices.",
            "Only SENCO1 and SENCO3 are restored from old_getco1_getco3_snapshot.json.",
            "A post-SENCO-write settle delay is enforced before GETCO readback to avoid overdriving fragile analyzer serial/flash handling.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="co2_senco13_pair_rollback",
        metadata=metadata,
        tables={
            "co2_senco13_pair_rollback_summary": summary_rows,
            "co2_senco13_pair_rollback_detail": detail_rows,
            "co2_senco13_pair_rollback_conclusion": [
                {
                    "overall_status": "failed" if failed else "success",
                    "started_at": start_ts,
                    "finished_at": end_ts,
                    "target_count": len(selected_ids),
                    "processed_count": len(summary_rows),
                    "success_count": sum(1 for row in summary_rows if row.get("status") == "rollback_readback_verified"),
                    "controls_water_or_gas_routes": False,
                    "writes_device_id": False,
                    "writes_senco1": True,
                    "writes_senco3": True,
                    "clears_senco": False,
                }
            ],
        },
    )
    database_sidecar = _write_database_sidecar(
        destination,
        write_dir=write_dir,
        snapshot_path=snapshot_path,
        outputs=outputs,
        summary_rows=summary_rows,
        finished_at=end_ts,
    )
    base._log(f"Controlled CO2 SENCO1/SENCO3 rollback report saved: {outputs['workbook']}")
    base._log(f"Rollback database sidecar saved: {database_sidecar}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
