from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Sequence

import pandas as pd

from ..exceptions import ConfigurationInvalidError, DataParseError

if TYPE_CHECKING:
    from gas_calibrator.devices.gas_analyzer import GasAnalyzer


@dataclass(frozen=True)
class AnalyzerDownloadTarget:
    analyzer: str
    port: str
    baudrate: int = 115200
    timeout: float = 1.0
    device_id: str = "000"


class CsvIoLogger:
    """Minimal CSV IO logger for standalone coefficient download tasks."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._handle,
            fieldnames=["timestamp", "port", "device", "direction", "command", "response", "error"],
        )
        self._writer.writeheader()

    def log_io(
        self,
        *,
        port: str,
        device: str,
        direction: str,
        command: Any = None,
        response: Any = None,
        error: Any = None,
    ) -> None:
        self._writer.writerow(
            {
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "port": str(port or ""),
                "device": str(device or ""),
                "direction": str(direction or ""),
                "command": "" if command is None else str(command),
                "response": "" if response is None else str(response),
                "error": "" if error is None else str(error),
            }
        )
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()


def _resolve_gas_analyzer_class() -> type["GasAnalyzer"]:
    try:
        module = import_module("gas_calibrator.devices.gas_analyzer")
    except ModuleNotFoundError as exc:
        dependency_name = str(getattr(exc, "name", "") or "gas_calibrator.devices.gas_analyzer")
        raise ImportError(
            "Cannot execute real analyzer coefficient download because driver dependency "
            f"'{dependency_name}' is unavailable while loading gas_calibrator.devices.gas_analyzer.GasAnalyzer. "
            "This path requires the real analyzer driver stack and should only be used in an engineer-controlled "
            "real-device environment."
        ) from exc
    try:
        return getattr(module, "GasAnalyzer")
    except AttributeError as exc:
        raise ImportError(
            "gas_calibrator.devices.gas_analyzer.GasAnalyzer is not available."
        ) from exc


def _normalize_analyzer_label(value: Any, *, index: Optional[int] = None) -> str:
    text = str(value or "").strip().upper()
    if text:
        return text
    if index is not None:
        return f"GA{index + 1:02d}"
    return ""


def load_download_plan(report_path: str | Path) -> list[dict[str, str]]:
    path = Path(report_path)
    if not path.exists():
        raise ConfigurationInvalidError("report_path", str(path), reason="系数报告不存在")

    try:
        frame = pd.read_excel(path, sheet_name="download_plan", dtype=str)
    except Exception as exc:  # pragma: no cover
        raise DataParseError("download_plan", reason=f"无法读取 download_plan: {exc}") from exc

    required = {"Analyzer", "Gas", "PrimaryCommand", "SecondaryCommand", "ModeEnterCommand", "ModeExitCommand"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise DataParseError("download_plan", reason=f"download_plan 缺少列: {', '.join(missing)}")

    rows: list[dict[str, str]] = []
    for row in frame.to_dict(orient="records"):
        analyzer = _normalize_analyzer_label(row.get("Analyzer"))
        if not analyzer:
            continue
        payload = {key: ("" if row.get(key) is None else str(row.get(key)).strip()) for key in frame.columns}
        payload["Analyzer"] = analyzer
        rows.append(payload)
    if not rows:
        raise DataParseError("download_plan", reason="download_plan 中没有可用下发记录")
    return rows


def load_download_targets(config_path: str | Path) -> list[AnalyzerDownloadTarget]:
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationInvalidError("config_path", str(path), reason="配置文件不存在")

    raw = json.loads(path.read_text(encoding="utf-8"))
    devices = raw.get("devices", {}) or {}
    configs = devices.get("gas_analyzers")
    if not isinstance(configs, list) or not configs:
        single = devices.get("gas_analyzer")
        configs = [single] if isinstance(single, dict) else []

    targets: list[AnalyzerDownloadTarget] = []
    for index, item in enumerate(configs):
        if not isinstance(item, dict):
            continue
        if not bool(item.get("enabled", True)):
            continue
        port = str(item.get("port") or "").strip()
        if not port:
            continue
        targets.append(
            AnalyzerDownloadTarget(
                analyzer=_normalize_analyzer_label(item.get("name"), index=index),
                port=port,
                baudrate=int(item.get("baud", item.get("baudrate", 115200))),
                timeout=float(item.get("timeout", 1.0)),
                device_id=str(item.get("device_id", "000")),
            )
        )
    if not targets:
        raise ConfigurationInvalidError("devices.gas_analyzers", None, reason="未找到可用的气体分析仪串口配置")
    return targets


def _send_broadcast_command(analyzer: GasAnalyzer, payload: str) -> bool:
    command = str(payload or "").strip()
    if not command:
        raise ValueError("payload is empty")
    acked = analyzer._send_config_with_retries(
        command,
        broadcast=True,
        require_ack=True,
        attempts=1 + max(0, int(getattr(analyzer, "CONFIG_ACK_RETRY_COUNT", 1))),
        retry_delay_s=float(getattr(analyzer, "CONFIG_ACK_RETRY_DELAY_S", 0.1)),
    )
    if not acked:
        analyzer._log_no_ack(command)
    return acked


def _group_plan_by_analyzer(plan_rows: Sequence[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in plan_rows:
        grouped.setdefault(_normalize_analyzer_label(row.get("Analyzer")), []).append(dict(row))
    for rows in grouped.values():
        rows.sort(key=lambda row: str(row.get("Gas") or "").upper())
    return grouped


def download_coefficients_to_analyzers(
    *,
    report_path: str | Path,
    config_path: str | Path,
    output_dir: str | Path,
    serial_factory: Optional[Any] = None,
) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    io_logger = CsvIoLogger(out_dir / "coefficient_download_io.csv")
    plan_rows = load_download_plan(report_path)
    grouped_plan = _group_plan_by_analyzer(plan_rows)
    targets = load_download_targets(config_path)

    results: list[dict[str, Any]] = []
    try:
        for target in targets:
            analyzer_plan = grouped_plan.get(target.analyzer, [])
            if not analyzer_plan:
                results.append(
                    {
                        "analyzer": target.analyzer,
                        "port": target.port,
                        "ok": False,
                        "skipped": True,
                        "reason": "no_download_plan",
                    }
                )
                continue

            gas_analyzer_class = _resolve_gas_analyzer_class()
            analyzer = gas_analyzer_class(
                port=target.port,
                baudrate=target.baudrate,
                timeout=target.timeout,
                device_id=target.device_id,
                io_logger=io_logger,
                serial_factory=serial_factory,
            )
            ok = False
            error_text = ""
            commands_sent = 0
            entered_mode2 = False
            try:
                analyzer.open()
                analyzer.set_comm_way_with_ack(False, require_ack=False)
                entered_mode2 = bool(analyzer.set_mode_with_ack(2, require_ack=True))
                if not entered_mode2:
                    raise RuntimeError("MODE_2_ACK_FAILED")
                for row in analyzer_plan:
                    for command_key in ("PrimaryCommand", "SecondaryCommand"):
                        command = str(row.get(command_key) or "").strip()
                        if not command:
                            continue
                        if not _send_broadcast_command(analyzer, command):
                            raise RuntimeError(f"COMMAND_ACK_FAILED:{command}")
                        commands_sent += 1
                if not analyzer.set_mode_with_ack(1, require_ack=True):
                    raise RuntimeError("MODE_1_ACK_FAILED")
                ok = True
            except Exception as exc:
                error_text = str(exc)
                if entered_mode2:
                    try:
                        analyzer.set_mode_with_ack(1, require_ack=True)
                    except Exception:
                        pass
            finally:
                try:
                    analyzer.close()
                except Exception:
                    pass

            results.append(
                {
                    "analyzer": target.analyzer,
                    "port": target.port,
                    "ok": ok,
                    "skipped": False,
                    "commands_sent": commands_sent,
                    "error": error_text,
                }
            )
    finally:
        io_logger.close()

    summary_path = out_dir / "coefficient_download_summary.json"
    summary_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "report_path": str(Path(report_path)),
        "config_path": str(Path(config_path)),
        "results": results,
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "download_summary": str(summary_path),
        "io_log": str(out_dir / "coefficient_download_io.csv"),
    }
