from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .run001_a1_analyzer_diagnostics import (
    _close_analyzer,
    _make_default_analyzer,
    _open_analyzer,
    _parse_line,
    _port_discovery_configs,
)
from .run001_a1_serial_assistant_probe import (
    SERIAL_ASSISTANT_KNOWN_DETECTED_IDS,
    _flush_input,
    _listen_stream_lines,
    _normalize_port,
)


TRUTH_AUDIT_SCHEMA_VERSION = "run001_a1.analyzer_id_truth_audit.1"
TRUTH_AUDIT_POLICY = "block_a1_current_storage_uses_frame_id_unique_key"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mode_from_payload(payload: Mapping[str, Any] | None) -> int:
    if not isinstance(payload, Mapping):
        return 0
    try:
        return int(float(payload.get("mode") or 0))
    except Exception:
        return 0


def _capture_truth_for_port(
    cfg: Mapping[str, Any],
    *,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    port = _normalize_port(cfg.get("configured_port") or cfg.get("port"))
    result: dict[str, Any] = {
        "port": port,
        "configured_port": port,
        "configured_expected_device_id": str(
            cfg.get("configured_device_id")
            or cfg.get("formal_configured_device_id_for_port")
            or cfg.get("expected_device_id")
            or ""
        ).strip(),
        "previous_mode1_discovery_id": SERIAL_ASSISTANT_KNOWN_DETECTED_IDS.get(port, ""),
        "read_only": True,
        "commands_sent": [],
        "port_open": False,
        "flush_before_capture": False,
        "bytes_received": 0,
        "raw_frame_count": 0,
        "mode1_frame_count": 0,
        "mode2_frame_count": 0,
        "parse_error_count": 0,
        "active_send_detected": False,
        "first_valid_mode2_frame": "",
        "observed_device_id_set": [],
        "stable_device_id": "",
        "mode1_observed_device_id_set": [],
        "mode1_id_used_as_a1_expected_id": False,
        "raw_frame_samples": [],
        "error": "",
    }
    analyzer: Any = None
    lines: list[str] = []
    try:
        factory = analyzer_factory or (lambda item: _make_default_analyzer(item, timeout_s))
        analyzer = factory(cfg)
        try:
            setattr(analyzer, "active_send", True)
        except Exception:
            pass
        _open_analyzer(analyzer)
        result["port_open"] = True
        result["flush_before_capture"] = _flush_input(analyzer)
        lines = _listen_stream_lines(analyzer, timeout_s)
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        if analyzer is not None:
            _close_analyzer(analyzer)

    result["raw_frame_samples"] = [str(line or "") for line in lines if str(line or "").strip()]
    result["raw_frame_count"] = len(result["raw_frame_samples"])
    result["bytes_received"] = sum(len(str(line or "").encode("utf-8")) for line in result["raw_frame_samples"])
    result["active_send_detected"] = bool(result["raw_frame_samples"])

    mode1_ids: list[str] = []
    mode2_ids: list[str] = []
    parse_errors = 0
    for line in result["raw_frame_samples"]:
        mode2_payload, parsed_payload = _parse_line(analyzer, str(line or "")) if analyzer is not None else (None, None)
        if mode2_payload:
            device_id = str(mode2_payload.get("id") or "").strip()
            if device_id:
                mode2_ids.append(device_id)
            if not result["first_valid_mode2_frame"]:
                result["first_valid_mode2_frame"] = str(line or "")
            continue
        if parsed_payload:
            mode = _mode_from_payload(parsed_payload)
            if mode == 1:
                device_id = str(parsed_payload.get("id") or "").strip()
                if device_id:
                    mode1_ids.append(device_id)
                continue
            if mode == 2:
                device_id = str(parsed_payload.get("id") or "").strip()
                if device_id:
                    mode2_ids.append(device_id)
                if not result["first_valid_mode2_frame"]:
                    result["first_valid_mode2_frame"] = str(line or "")
                continue
        if str(line or "").strip():
            parse_errors += 1

    result["mode1_frame_count"] = len(mode1_ids)
    result["mode2_frame_count"] = len(mode2_ids)
    result["parse_error_count"] = parse_errors
    result["observed_device_id_set"] = sorted(set(mode2_ids))
    result["mode1_observed_device_id_set"] = sorted(set(mode1_ids))
    if len(set(mode2_ids)) == 1:
        result["stable_device_id"] = mode2_ids[0]
    result["mode2_device_id_counts"] = dict(Counter(mode2_ids))
    result["mode1_device_id_counts"] = dict(Counter(mode1_ids))
    result["mode1_parser_id_matches_mode2_truth"] = bool(
        result["previous_mode1_discovery_id"]
        and result["stable_device_id"]
        and result["previous_mode1_discovery_id"] == result["stable_device_id"]
    )
    result["mode1_parser_id_trust_for_a1"] = "not_used_for_a1_expected_id"
    return result


def _duplicate_summary(port_results: list[dict[str, Any]]) -> dict[str, Any]:
    by_id: dict[str, list[str]] = defaultdict(list)
    for item in port_results:
        stable = str(item.get("stable_device_id") or "").strip()
        port = _normalize_port(item.get("port"))
        if stable and port:
            by_id[stable].append(port)
    duplicates = {device_id: ports for device_id, ports in by_id.items() if len(ports) > 1}
    duplicate_values = sorted(duplicates)
    first_value = duplicate_values[0] if duplicate_values else ""
    return {
        "duplicate_device_id_detected": bool(duplicates),
        "duplicate_device_id_value": first_value,
        "duplicate_device_id_ports": duplicates.get(first_value, []),
        "duplicate_device_id_map": duplicates,
        "duplicate_device_id_policy": TRUTH_AUDIT_POLICY if duplicates else "not_applicable",
        "duplicate_device_id_status": "blocked" if duplicates else "not_detected",
        "duplicate_policy_basis": (
            "samples_runtime import can use frame id as analyzer_id, and storage has point_id+analyzer_id+sample_index uniqueness"
            if duplicates
            else ""
        ),
    }


def build_analyzer_id_truth_audit_payload(
    raw_cfg: Mapping[str, Any],
    *,
    ports: Optional[list[str]] = None,
    read_only: bool = True,
    timeout_s: float = 30.0,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    if not read_only:
        raise ValueError("Analyzer ID truth audit is read-only only")
    requested_ports = [_normalize_port(item) for item in list(ports or []) if _normalize_port(item)]
    selected = _port_discovery_configs(raw_cfg, requested_ports)
    results = [
        _capture_truth_for_port(
            cfg,
            analyzer_factory=analyzer_factory,
            timeout_s=timeout_s,
        )
        for cfg in selected
    ]
    duplicates = _duplicate_summary(results)
    mode2_ready_count = sum(1 for item in results if item.get("mode2_frame_count", 0) > 0 and item.get("stable_device_id"))
    return {
        "schema_version": TRUTH_AUDIT_SCHEMA_VERSION,
        "artifact_type": "run001_a1_analyzer_id_truth_audit",
        "generated_at": _utc_now(),
        "run_id": "Run-001/A1",
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "a1_execute_invoked": False,
        "a2_invoked": False,
        "h2o_invoked": False,
        "full_group_invoked": False,
        "requested_ports": requested_ports,
        "timeout_s": float(timeout_s),
        "mode1_discovery_ids": {
            port: SERIAL_ASSISTANT_KNOWN_DETECTED_IDS.get(port, "")
            for port in requested_ports
        },
        "mode1_ids_not_authoritative_for_a1": True,
        "mode2_truth_source": "passive_raw_frame_capture_after_flush",
        "analyzers": results,
        **duplicates,
        "summary": {
            "total": len(results),
            "mode2_ready": mode2_ready_count,
            "failed": len(results) - mode2_ready_count,
            "duplicate_device_id_detected": duplicates["duplicate_device_id_detected"],
            "duplicate_device_id_status": duplicates["duplicate_device_id_status"],
            "not_a1_pass_evidence": True,
            "not_real_acceptance_evidence": True,
        },
        "not_a1_pass_evidence": True,
        "not_real_acceptance_evidence": True,
    }


def render_analyzer_id_truth_audit_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 analyzer ID truth audit",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- read_only: {payload.get('read_only')}",
        f"- passive_listen_only: {payload.get('passive_listen_only')}",
        f"- commands_sent: {payload.get('commands_sent')}",
        f"- mode2_truth_source: {payload.get('mode2_truth_source')}",
        f"- duplicate_device_id_detected: {payload.get('duplicate_device_id_detected')}",
        f"- duplicate_device_id_value: {payload.get('duplicate_device_id_value') or '-'}",
        f"- duplicate_device_id_ports: {payload.get('duplicate_device_id_ports')}",
        f"- duplicate_device_id_policy: {payload.get('duplicate_device_id_policy')}",
        "",
        "## Ports",
    ]
    for item in list(payload.get("analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('port')}",
                f"- bytes_received: {item.get('bytes_received')}",
                f"- flush_before_capture: {item.get('flush_before_capture')}",
                f"- mode2_frame_count: {item.get('mode2_frame_count')}",
                f"- observed_device_id_set: {item.get('observed_device_id_set')}",
                f"- stable_device_id: {item.get('stable_device_id') or '-'}",
                f"- previous_mode1_discovery_id: {item.get('previous_mode1_discovery_id') or '-'}",
                f"- mode1_parser_id_matches_mode2_truth: {item.get('mode1_parser_id_matches_mode2_truth')}",
                f"- raw_sample_path: {item.get('raw_sample_path') or '-'}",
                f"- parse_error_count: {item.get('parse_error_count')}",
                f"- error: {item.get('error') or '-'}",
            ]
        )
    lines.extend(["", "This audit is not A1 PASS evidence and does not authorize A2, H2O, full group, cutover, or writes.", ""])
    return "\n".join(lines)


def write_analyzer_id_truth_audit_artifacts(
    output_dir: str | Path,
    payload: Mapping[str, Any],
) -> dict[str, str]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    enriched = dict(payload)
    analyzers = [dict(item) for item in list(enriched.get("analyzers") or [])]
    written: dict[str, str] = {}
    for item in analyzers:
        port = _normalize_port(item.get("port"))
        sample_path = directory / f"raw_frame_samples_{port}.txt"
        sample_path.write_text("\n".join(str(line or "") for line in list(item.get("raw_frame_samples") or [])) + "\n", encoding="utf-8")
        item["raw_sample_path"] = str(sample_path)
        written[f"raw_frame_samples_{port}"] = str(sample_path)
    enriched["analyzers"] = analyzers
    json_path = directory / "analyzer_id_truth_audit.json"
    md_path = directory / "analyzer_id_truth_audit.md"
    json_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_analyzer_id_truth_audit_report(enriched), encoding="utf-8")
    written["analyzer_id_truth_audit_json"] = str(json_path)
    written["analyzer_id_truth_audit_md"] = str(md_path)
    return written
