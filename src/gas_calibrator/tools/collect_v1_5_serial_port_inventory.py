"""Collect V1.5 serial-port inventory evidence without opening COM ports."""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from ..v1_5.orchestration.serial_port_binding import build_v1_5_serial_port_inventory


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


def _parse_ports_arg(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]


def _read_ports_json(path: str | None) -> Any:
    if not path:
        return None
    return json.loads(Path(path).resolve().read_text(encoding="utf-8"))


def _collect_windows_list_ports() -> list[dict[str, Any]]:
    """List Windows serial ports without opening them or sending commands."""

    try:
        from serial.tools import list_ports
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("pyserial is required for --from-windows-list-ports") from exc

    rows: list[dict[str, Any]] = []
    for item in list_ports.comports():
        rows.append(
            {
                "port": item.device,
                "description": item.description,
                "hwid": item.hwid,
                "manufacturer": getattr(item, "manufacturer", None) or "",
                "product": getattr(item, "product", None) or "",
                "serial_number": getattr(item, "serial_number", None) or "",
            }
        )
    return rows


def _merge_port_sources(*sources: Any) -> list[Any]:
    out: list[Any] = []
    for source in sources:
        if not source:
            continue
        if isinstance(source, Mapping) and "ports" in source:
            out.extend(source["ports"])
        elif isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
            out.extend(source)
        else:
            out.append(source)
    return out


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect V1.5 serial-port inventory evidence.")
    parser.add_argument("--output-dir", required=True, help="Output directory for inventory artifacts.")
    parser.add_argument(
        "--available-ports",
        default=None,
        help="Comma-separated COM ports supplied by UI/operator, e.g. COM16,COM17,COM35.",
    )
    parser.add_argument(
        "--ports-json",
        default=None,
        help="Optional JSON inventory or port list supplied by UI/operator.",
    )
    parser.add_argument(
        "--protocol-inventory-json",
        default=None,
        help="Optional JSON file with externally collected port protocol identities.",
    )
    parser.add_argument(
        "--from-windows-list-ports",
        action="store_true",
        help="Use pyserial list_ports to enumerate OS COM paths. This does not open COM ports.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    explicit_ports = _parse_ports_arg(args.available_ports)
    json_ports = _read_ports_json(args.ports_json)
    windows_ports = _collect_windows_list_ports() if args.from_windows_list_ports else []
    protocol_inventory = _read_ports_json(args.protocol_inventory_json)
    source = (
        "windows_list_ports"
        if args.from_windows_list_ports and not explicit_ports and not json_ports
        else "provided_or_ui"
    )
    inventory = build_v1_5_serial_port_inventory(
        _merge_port_sources(explicit_ports, json_ports, windows_ports),
        protocol_inventory=protocol_inventory,
        source=source,
    )

    inventory_path = destination / "runtime_serial_port_inventory.json"
    inventory_path.write_text(json.dumps(inventory, ensure_ascii=False, indent=2), encoding="utf-8")
    csv_path = destination / "runtime_serial_port_inventory.csv"
    _write_csv(csv_path, inventory["ports"])
    summary = {
        key: value
        for key, value in inventory.items()
        if key
        not in {
            "ports",
        }
    }
    summary.update({"inventory_json": str(inventory_path), "inventory_csv": str(csv_path)})
    summary_path = destination / "runtime_serial_port_inventory_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
