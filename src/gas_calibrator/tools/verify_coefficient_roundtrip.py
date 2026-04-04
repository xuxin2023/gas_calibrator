"""Coefficient readback / roundtrip verification.

This is a sidecar safety tool. By default it only reads and exports current
coefficients. Any device write requires an explicit opt-in flag.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..config import load_config
from ..devices import GasAnalyzer
from ..validation.reporting import ValidationMetadata, write_validation_report


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_group_list(raw: str | None) -> List[int]:
    groups: List[int] = []
    for part in str(raw or "").split(","):
        text = part.strip()
        if not text:
            continue
        groups.append(int(text))
    return groups or [1, 2, 3, 4]


def _resolve_analyzer_cfg(
    cfg: Mapping[str, Any],
    *,
    analyzer: Optional[str],
) -> Dict[str, Any]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_cfg = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    target = str(analyzer or "").strip().upper()
    if isinstance(gas_cfg, list) and gas_cfg:
        for idx, item in enumerate(gas_cfg, start=1):
            if not isinstance(item, Mapping) or not item.get("enabled", True):
                continue
            name = str(item.get("name") or f"GA{idx:02d}").upper()
            device_id = str(item.get("device_id", "") or "").upper()
            if not target or target in {name, device_id, str(idx)}:
                return dict(item)
    single_cfg = devices_cfg.get("gas_analyzer", {}) if isinstance(devices_cfg, Mapping) else {}
    if isinstance(single_cfg, Mapping) and single_cfg.get("enabled", False):
        return dict(single_cfg)
    raise RuntimeError(f"Analyzer selection not found: {analyzer}")


def _read_groups(ga: GasAnalyzer, groups: List[int]) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    for group in groups:
        out[int(group)] = ga.read_coefficient_group(int(group))
    return out


def _rows_from_groups(stage: str, groups: Mapping[int, Mapping[str, float]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group, values in sorted(groups.items()):
        for key, value in sorted(values.items(), key=lambda item: int(str(item[0]).lstrip("C"))):
            rows.append(
                {
                    "stage": stage,
                    "group": int(group),
                    "coefficient_name": f"SENCO{int(group)}.{key}",
                    "value": float(value),
                }
            )
    return rows


def _compare_groups(
    before: Mapping[int, Mapping[str, float]],
    written: Mapping[int, Mapping[str, float]],
    readback: Mapping[int, Mapping[str, float]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    all_groups = sorted({*before.keys(), *written.keys(), *readback.keys()})
    for group in all_groups:
        keys = sorted(
            {
                *before.get(group, {}).keys(),
                *written.get(group, {}).keys(),
                *readback.get(group, {}).keys(),
            },
            key=lambda item: int(str(item).lstrip("C")),
        )
        for key in keys:
            before_value = before.get(group, {}).get(key)
            written_value = written.get(group, {}).get(key)
            readback_value = readback.get(group, {}).get(key)
            diff = None
            if written_value is not None and readback_value is not None:
                diff = float(readback_value) - float(written_value)
            rows.append(
                {
                    "group": group,
                    "coefficient_name": f"SENCO{group}.{key}",
                    "before_value": before_value,
                    "written_value": written_value,
                    "readback_value": readback_value,
                    "diff": diff,
                }
            )
    return rows


def _load_write_payload(path: str | Path) -> Dict[int, Dict[str, float]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    out: Dict[int, Dict[str, float]] = {}
    for key, values in dict(raw).items():
        group = int(key)
        if isinstance(values, Mapping):
            out[group] = {str(name): float(value) for name, value in values.items()}
        else:
            out[group] = {f"C{idx}": float(value) for idx, value in enumerate(list(values))}
    return out


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read back analyzer coefficients and optionally perform a same-value roundtrip.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--analyzer", default="", help="Analyzer label, device ID, or 1-based index.")
    parser.add_argument("--groups", default="1,2,3,4", help="Coefficient groups to read, comma-separated.")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--write-back-same", action="store_true", help="Write back exactly what was read, then read again.")
    parser.add_argument("--write-from-json", default=None, help="Optional JSON payload to write instead of the current values.")
    parser.add_argument(
        "--allow-write-modified",
        action="store_true",
        help="Required with --write-from-json. Prevents accidental modified writes.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    analyzer_cfg = _resolve_analyzer_cfg(cfg, analyzer=args.analyzer)
    groups = _parse_group_list(args.groups)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else Path(cfg["paths"]["output_dir"]).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.write_from_json and not args.allow_write_modified:
        _log("Refusing modified write: pass --allow-write-modified together with --write-from-json.")
        return 2

    ga = GasAnalyzer(
        analyzer_cfg["port"],
        analyzer_cfg.get("baud", 115200),
        device_id=str(analyzer_cfg.get("device_id", "000")),
    )
    before_groups: Dict[int, Dict[str, float]] = {}
    written_groups: Dict[int, Dict[str, float]] = {}
    readback_groups: Dict[int, Dict[str, float]] = {}
    try:
        _log("Coefficient roundtrip tool is in safe mode by default. No device write happens unless explicitly requested.")
        ga.open()
        before_groups = _read_groups(ga, groups)

        if args.write_from_json:
            written_groups = _load_write_payload(args.write_from_json)
            ga.set_mode(2)
            for group, values in sorted(written_groups.items()):
                ordered = [value for _name, value in sorted(values.items(), key=lambda item: int(str(item[0]).lstrip("C")))]
                ga.set_senco(int(group), *ordered)
            ga.set_mode(1)
            readback_groups = _read_groups(ga, sorted(written_groups.keys()))
        elif args.write_back_same:
            written_groups = {group: dict(values) for group, values in before_groups.items()}
            ga.set_mode(2)
            for group, values in sorted(written_groups.items()):
                ordered = [value for _name, value in sorted(values.items(), key=lambda item: int(str(item[0]).lstrip("C")))]
                ga.set_senco(int(group), *ordered)
            ga.set_mode(1)
            readback_groups = _read_groups(ga, groups)
        else:
            readback_groups = {group: dict(values) for group, values in before_groups.items()}

        tables = {
            "coefficient_before": _rows_from_groups("before", before_groups),
            "coefficient_written": _rows_from_groups("written", written_groups or before_groups),
            "coefficient_readback": _rows_from_groups("readback", readback_groups),
            "coefficient_roundtrip": _compare_groups(
                before_groups,
                written_groups or before_groups,
                readback_groups,
            ),
        }
        diffs = [abs(float(row["diff"])) for row in tables["coefficient_roundtrip"] if row.get("diff") is not None]
        tables["conclusion_summary"] = [
            {
                "risk_level": "pass" if not diffs or max(diffs) == 0.0 else "warn",
                "max_abs_diff": max(diffs) if diffs else 0.0,
                "write_mode": (
                    "modified_write"
                    if args.write_from_json
                    else "same_value_write"
                    if args.write_back_same
                    else "read_only"
                ),
            }
        ]
        metadata = ValidationMetadata(
            tool_name="verify_coefficient_roundtrip",
            created_at=datetime.now().isoformat(timespec="seconds"),
            analyzers=[str(analyzer_cfg.get("name") or args.analyzer or analyzer_cfg.get("device_id", ""))],
            input_paths=[str(Path(args.config).resolve())],
            output_dir=str(output_dir),
            config_path=str(Path(args.config).resolve()),
            config_summary={
                "groups": groups,
                "write_back_same": bool(args.write_back_same),
                "write_from_json": str(Path(args.write_from_json).resolve()) if args.write_from_json else "",
                "allow_write_modified": bool(args.allow_write_modified),
            },
            notes=[
                "Default mode is read-only.",
                "Any write path is sidecar-only and requires explicit flags.",
            ],
        )
        outputs = write_validation_report(
            output_dir,
            prefix=f"coefficient_roundtrip_{str(analyzer_cfg.get('device_id', '000'))}",
            metadata=metadata,
            tables=tables,
        )
        _log(f"Coefficient roundtrip report saved: {outputs['workbook']}")
        return 0
    except Exception as exc:
        _log(f"Coefficient roundtrip failed: {exc}")
        return 1
    finally:
        try:
            ga.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
