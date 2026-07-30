from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping


def runtime_setup_commands() -> list[dict[str, object]]:
    pairs = (
        ("set_comm_way_inactive", "SETCOMWAY,YGAS,FFF,0"),
        ("set_mode2", "MODE,YGAS,FFF,2"),
        ("set_active_frequency", "FTD,YGAS,FFF,01"),
        ("set_average1_filter", "AVERAGE1,YGAS,FFF,49"),
        ("set_average2_filter", "AVERAGE2,YGAS,FFF,49"),
        ("set_comm_way_active", "SETCOMWAY,YGAS,FFF,1"),
    )
    return [
        {
            "step": index,
            "action": action,
            "command_preview": command,
            "ack_required": True,
        }
        for index, (action, command) in enumerate(pairs, start=1)
    ]


def runtime_setup_result_payload(
    analyzers: list[Mapping[str, object]],
    *,
    run_id: str,
) -> dict[str, object]:
    commands = runtime_setup_commands()
    safety = {
        "writes_senco": False,
        "writes_device_id": False,
        "writes_sn": False,
        "controls_gas_route": False,
        "controls_water_route": False,
        "controls_pressure": False,
        "controls_temperature": False,
        "runs_sampling": False,
        "runs_fitting": False,
        "not_real_acceptance_evidence": True,
    }
    contract = {
        "command_gap_s": 1.0,
        "mode": 2,
        "active_send": True,
        "ftd_hz": 1,
        "average1_target": 49,
        "average2_target": 49,
    }
    results: list[dict[str, object]] = []
    normalized_analyzers: list[dict[str, object]] = []
    for index, source in enumerate(analyzers, start=1):
        port = str(source["port"])
        protocol_id = str(source["protocol_device_id"])
        sn_code = str(source["sn_code"])
        slot = str(source.get("slot") or source.get("ga_label") or f"GA{index:02d}")
        analyzer = {
            "slot": slot,
            "port": port,
            "protocol_device_id": protocol_id,
            "sn_code": sn_code,
            "device_code": sn_code,
        }
        normalized_analyzers.append(analyzer)
        frames = [
            {
                "parsed": {"mode": 2, "id": protocol_id},
                "ok": True,
            }
        ]
        rate = {
            "enabled": True,
            "target_hz": 1,
            "measure_s": 6.0,
            "valid_mode2_lines": 6,
            "approx_hz": 1.0,
            "min_hz": 0.7,
            "max_hz": 1.3,
            "ok": True,
        }
        events = [
            {
                **command,
                "ack_received": True,
                "ok": True,
            }
            for command in commands
        ]
        results.append(
            {
                **analyzer,
                "status": "ready",
                "sn_readback": sn_code,
                "identity_before": {"mode": 2, "id": protocol_id},
                "identity_after": {"mode": 2, "id": protocol_id},
                "runtime_setup_events": events,
                "mode2_frames": frames,
                "active_upload_rate": rate,
                "runtime_setup_attempt_count": 1,
                "runtime_setup_attempts": [
                    {
                        "attempt": 1,
                        "status": "ready",
                        "runtime_setup_events": events,
                        "mode2_frames": frames,
                        "active_upload_rate": rate,
                    }
                ],
            }
        )
    return {
        "schema_version": "v1_5_analyzer_runtime_setup_result_v0",
        "run_id": run_id,
        "status": "ready",
        "evidence_source": "real_device_runtime_setup",
        "execution_mode": "controlled_real_com",
        "engineering_setup_only": True,
        "not_real_acceptance_evidence": True,
        "evidence_paths": {},
        "boundary": {
            "opens_com_ports": True,
            "sends_device_commands": True,
            "writes_runtime_settings": True,
            "all_configuration_commands_require_ack": True,
            "writes_senco": False,
            "writes_device_id": False,
            "writes_sn": False,
        },
        "plan": {
            "schema_version": "v1_5_analyzer_runtime_setup_plan_v0",
            "safety": safety,
            "contract": contract,
            "commands": commands,
            "analyzers": normalized_analyzers,
        },
        "results": results,
    }


def write_runtime_setup_result(
    path: Path,
    analyzers: list[Mapping[str, object]],
    *,
    run_id: str,
) -> Path:
    path.write_text(
        json.dumps(
            runtime_setup_result_payload(analyzers, run_id=run_id),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
