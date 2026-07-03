"""Prepare optional V1.5 runtime serial-port binding evidence.

Default behavior is intentionally no-op.  The COM24-COM31 <-> COM16-COM23
reference-device bank shift is applied only when explicitly enabled and an
available-port inventory is supplied by the caller/UI.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from ..config import load_config
from ..v1_5.orchestration.serial_port_binding import resolve_reference_port_bank_shift


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: list[str] = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _config_default_enabled(cfg: Mapping[str, Any]) -> bool:
    workflow = cfg.get("workflow") if isinstance(cfg, Mapping) else {}
    binding = workflow.get("serial_port_binding", {}) if isinstance(workflow, Mapping) else {}
    if not isinstance(binding, Mapping):
        return False
    return bool(binding.get("reference_bank_shift_enabled", False))


def _config_default_require_protocol_match(cfg: Mapping[str, Any]) -> bool:
    workflow = cfg.get("workflow") if isinstance(cfg, Mapping) else {}
    binding = workflow.get("serial_port_binding", {}) if isinstance(workflow, Mapping) else {}
    if not isinstance(binding, Mapping):
        return False
    return bool(binding.get("require_protocol_match", False))


def _load_protocol_inventory(path: str | None) -> Any:
    if not path:
        return None
    return json.loads(Path(path).resolve().read_text(encoding="utf-8"))


def _load_port_inventory(path: str | None) -> Any:
    if not path:
        return None
    return json.loads(Path(path).resolve().read_text(encoding="utf-8"))


def _ports_from_inventory(value: Any) -> list[str]:
    if not value:
        return []
    ports = value.get("ports", value) if isinstance(value, Mapping) else value
    out: list[str] = []
    if isinstance(ports, Mapping):
        out.extend(str(key) for key in ports)
    elif isinstance(ports, Sequence) and not isinstance(ports, (str, bytes, bytearray)):
        for item in ports:
            if isinstance(item, Mapping):
                port = item.get("port") or item.get("runtime_port") or item.get("configured_port") or item.get("com")
                if port:
                    out.append(str(port))
            elif item:
                out.append(str(item))
    elif ports:
        out.append(str(ports))
    return out


def _protocol_from_port_inventory(value: Any) -> Any:
    if not value:
        return None
    ports = value.get("ports", value) if isinstance(value, Mapping) else value
    if isinstance(ports, Mapping):
        return ports
    if isinstance(ports, Sequence) and not isinstance(ports, (str, bytes, bytearray)):
        rows = []
        for item in ports:
            if not isinstance(item, Mapping):
                continue
            if item.get("observed_role") or item.get("role"):
                rows.append(item)
        return rows or None
    return None


def _merge_available_ports(*values: Any) -> str | None:
    out: list[str] = []
    for value in values:
        if not value:
            continue
        if isinstance(value, str):
            out.extend(item.strip() for item in value.replace(";", ",").split(",") if item.strip())
        else:
            out.extend(str(item) for item in value if str(item or "").strip())
    if not out:
        return None
    return ",".join(dict.fromkeys(out))


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare V1.5 runtime serial-port binding evidence.")
    parser.add_argument("--config", required=True, help="Input V1.5 runtime config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for runtime config and evidence.")
    parser.add_argument(
        "--enable-reference-bank-shift",
        action="store_true",
        help="Enable COM24-COM31 <-> COM16-COM23 reference-device bank-shift binding.",
    )
    parser.add_argument(
        "--available-ports",
        default=None,
        help="Comma-separated available ports supplied by UI or a separate inventory step.",
    )
    parser.add_argument(
        "--protocol-inventory-json",
        default=None,
        help="Optional JSON file with externally collected port protocol identities.",
    )
    parser.add_argument(
        "--port-inventory-json",
        default=None,
        help="Optional runtime_serial_port_inventory.json from collect_v1_5_serial_port_inventory.",
    )
    parser.add_argument(
        "--require-protocol-match",
        action="store_true",
        help="Require runtime ports to match expected reference-device roles from protocol inventory.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    enabled = bool(args.enable_reference_bank_shift or _config_default_enabled(cfg))
    require_protocol_match = bool(args.require_protocol_match or _config_default_require_protocol_match(cfg))
    port_inventory = _load_port_inventory(args.port_inventory_json)
    protocol_inventory = _load_protocol_inventory(args.protocol_inventory_json) or _protocol_from_port_inventory(
        port_inventory
    )
    available_ports = _merge_available_ports(args.available_ports, _ports_from_inventory(port_inventory))
    result = resolve_reference_port_bank_shift(
        cfg,
        enabled=enabled,
        available_ports=available_ports,
        protocol_inventory=protocol_inventory,
        require_protocol_match=require_protocol_match,
    )

    config_path = destination / "runtime_serial_port_bound_config.json"
    config_path.write_text(json.dumps(result.config, ensure_ascii=False, indent=2), encoding="utf-8")
    evidence_path = destination / "runtime_serial_port_binding_evidence.csv"
    _write_csv(evidence_path, result.evidence_rows)
    summary = result.summary()
    summary.update(
        {
            "input_config": str(cfg_path),
            "runtime_config": str(config_path),
            "evidence_csv": str(evidence_path),
        }
    )
    summary_path = destination / "runtime_serial_port_binding_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 1 if result.status == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
