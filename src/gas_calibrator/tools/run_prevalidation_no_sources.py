"""Prevalidation orchestrator for no-gas-cylinder / no-humidity-generator conditions.

This sidecar CLI does not change the V1 production workflow timing or entry.
It only orchestrates existing validation tools so the bench can pre-check
sampling, export, summary, and coefficient roundtrip chains before final
real-gas acceptance is available again.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..config import load_config
from . import validate_dry_collect, validate_offline_run, validate_pressure_only, verify_coefficient_roundtrip


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run prevalidation under no-gas-cylinder / no-humidity-generator conditions."
    )
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=None, help="Optional root output directory override.")
    parser.add_argument("--offline-run-dir", default=None, help="Historical run directory for offline validation.")
    parser.add_argument("--skip-offline", action="store_true")
    parser.add_argument("--skip-dry-collect", action="store_true")
    parser.add_argument("--include-pressure", action="store_true", help="Also run pressure-only validation.")
    parser.add_argument("--include-roundtrip", action="store_true", help="Also run coefficient roundtrip validation.")
    parser.add_argument(
        "--allow-write-back-same",
        action="store_true",
        help="Only meaningful with --include-roundtrip. Enables same-value write/readback verification.",
    )
    parser.add_argument("--analyzer", default="", help="Analyzer label/device-id/index for roundtrip validation.")
    parser.add_argument("--pressure-points", default="ambient", help="Pressure-only step metadata points.")
    parser.add_argument("--count", type=int, default=None, help="Optional sample count override for sidecar steps.")
    parser.add_argument("--interval-s", type=float, default=None, help="Optional sample interval override for sidecar steps.")
    parser.add_argument("--temp-set", type=float, default=20.0, help="Dry-collect metadata temperature setpoint.")
    parser.add_argument("--pressure-target-hpa", type=float, default=None, help="Dry-collect metadata pressure target.")
    parser.add_argument("--no-prompt", action="store_true", help="Disable manual prompt in pressure-only mode.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after the first failed step.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _configured_analyzers(cfg: Mapping[str, Any]) -> List[str]:
    analyzers: List[str] = []
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_cfg = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    if isinstance(gas_cfg, list):
        for idx, item in enumerate(gas_cfg, start=1):
            if not isinstance(item, Mapping) or not item.get("enabled", True):
                continue
            name = str(item.get("name") or f"GA{idx:02d}").strip()
            device_id = str(item.get("device_id", "") or "").strip()
            analyzers.append(f"{name}({device_id})" if device_id else name)
    if not analyzers:
        single_cfg = devices_cfg.get("gas_analyzer", {}) if isinstance(devices_cfg, Mapping) else {}
        if isinstance(single_cfg, Mapping) and single_cfg.get("enabled", False):
            name = str(single_cfg.get("name") or "GA01").strip()
            device_id = str(single_cfg.get("device_id", "") or "").strip()
            analyzers.append(f"{name}({device_id})" if device_id else name)
    return analyzers


def _collect_artifacts(output_dir: Path) -> List[str]:
    if not output_dir.exists():
        return []
    artifacts: List[str] = []
    for path in sorted(output_dir.rglob("*")):
        if path.is_file():
            artifacts.append(str(path))
    return artifacts


def _step_result(
    *,
    name: str,
    status: str,
    output_dir: Path,
    write_device: bool,
    argv: List[str],
    return_code: Optional[int] = None,
    error: str = "",
) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "return_code": return_code,
        "write_device": bool(write_device),
        "output_dir": str(output_dir),
        "argv": list(argv),
        "artifacts": _collect_artifacts(output_dir),
        "error": error,
    }


def _run_step(
    *,
    name: str,
    output_dir: Path,
    fn,
    argv: List[str],
    write_device: bool,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    _log(f"[{name}] start")
    _log(f"[{name}] write_device={'YES' if write_device else 'NO'}")
    _log(f"[{name}] output_dir={output_dir}")
    try:
        return_code = int(fn(argv))
    except Exception as exc:
        _log(f"[{name}] failed: {exc}")
        return _step_result(
            name=name,
            status="FAIL",
            output_dir=output_dir,
            write_device=write_device,
            argv=argv,
            return_code=None,
            error=str(exc),
        )

    status = "PASS" if return_code == 0 else "FAIL"
    if status == "PASS":
        _log(f"[{name}] pass")
    else:
        _log(f"[{name}] failed with return_code={return_code}")
    return _step_result(
        name=name,
        status=status,
        output_dir=output_dir,
        write_device=write_device,
        argv=argv,
        return_code=return_code,
        error="",
    )


def _write_summary_files(root: Path, payload: Dict[str, Any]) -> Dict[str, Path]:
    summary_json = root / "summary.json"
    summary_md = root / "summary.md"
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: List[str] = []
    lines.append("# 无气瓶 / 无湿度发生器预验证汇总")
    lines.append("")
    lines.append("这是一套无气瓶 / 无湿度发生器条件下的预验证，不替代最终实气 acceptance。")
    lines.append("")
    lines.append("## 基本信息")
    lines.append(f"- 时间: {payload.get('created_at', '')}")
    lines.append(f"- 配置: {payload.get('config_path', '')}")
    lines.append(f"- 分析仪: {', '.join(payload.get('analyzers', [])) or '未解析到'}")
    lines.append(f"- 包含 pressure-only: {payload.get('include_pressure_only', False)}")
    lines.append(f"- 包含 roundtrip: {payload.get('include_roundtrip', False)}")
    lines.append(f"- roundtrip 可写回相同系数: {payload.get('allow_write_back_same', False)}")
    lines.append(f"- 总结论: {payload.get('overall_status', '')}")
    lines.append("")
    lines.append("## 各步骤状态")
    for step in payload.get("steps", []):
        lines.append(
            f"- {step.get('name')}: {step.get('status')} | 写设备={step.get('write_device')} | 输出={step.get('output_dir')}"
        )
        if step.get("error"):
            lines.append(f"  错误: {step.get('error')}")
        artifacts = step.get("artifacts") or []
        if artifacts:
            lines.append(f"  关键文件: {artifacts[0]}")
    lines.append("")
    lines.append("## 建议人工检查项")
    lines.append("- 优先看各步骤目录里的 `frame_quality_summary.csv` 与 `pressure_source_check.csv`。")
    lines.append("- 优先看 run 目录中的 `analyzer_summary.csv` / `分析仪汇总_*.csv` 是否完整。")
    lines.append("- 在日志/IO 中搜索关键字: `sample export failed`, `point export failed`, `analyzer-summary-csv`。")
    lines.append("- 不要在程序运行过程中打开正在写入的 xlsx，优先检查 csv 和日志。")
    lines.append("")
    lines.append("## 说明")
    lines.append("- 该预验证只验证程序链路、采样链路、落盘链路和只读/安全 roundtrip 链路。")
    lines.append("- 它不能验证 CO2 / H2O 正式点准确性，也不能替代最终 acceptance。")
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"summary_json": summary_json, "summary_md": summary_md}


def _overall_status(steps: List[Dict[str, Any]]) -> str:
    statuses = [str(step.get("status") or "").upper() for step in steps]
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    if any(status in {"WARN", "SKIP"} for status in statuses):
        return "WARN"
    return "PASS"


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    base_output = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else Path(cfg["paths"]["output_dir"]).resolve() / f"prevalidation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    base_output.mkdir(parents=True, exist_ok=True)

    steps: List[Dict[str, Any]] = []

    if not args.skip_offline:
        offline_dir = base_output / "offline"
        if not args.offline_run_dir:
            steps.append(
                _step_result(
                    name="offline",
                    status="WARN",
                    output_dir=offline_dir,
                    write_device=False,
                    argv=[],
                    error="offline step skipped because --offline-run-dir was not provided",
                )
            )
        else:
            offline_argv = [
                "--run-dir",
                str(Path(args.offline_run_dir).resolve()),
                "--config",
                str(Path(args.config).resolve()),
                "--output-dir",
                str(offline_dir),
            ]
            steps.append(
                _run_step(
                    name="offline",
                    output_dir=offline_dir,
                    fn=validate_offline_run.main,
                    argv=offline_argv,
                    write_device=False,
                )
            )
            if args.fail_fast and steps[-1]["status"] == "FAIL":
                payload = {
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "config_path": str(Path(args.config).resolve()),
                    "output_dir": str(base_output),
                    "analyzers": _configured_analyzers(cfg),
                    "include_pressure_only": bool(args.include_pressure),
                    "include_roundtrip": bool(args.include_roundtrip),
                    "allow_write_back_same": bool(args.allow_write_back_same),
                    "steps": steps,
                    "overall_status": _overall_status(steps),
                }
                _write_summary_files(base_output, payload)
                return 1

    if not args.skip_dry_collect:
        dry_dir = base_output / "dry_collect"
        dry_argv = [
            "--config",
            str(Path(args.config).resolve()),
            "--output-dir",
            str(dry_dir),
            "--temp-set",
            f"{float(args.temp_set):g}",
        ]
        if args.count is not None:
            dry_argv += ["--count", str(int(args.count))]
        if args.interval_s is not None:
            dry_argv += ["--interval-s", str(float(args.interval_s))]
        if args.pressure_target_hpa is not None:
            dry_argv += ["--pressure-target-hpa", str(float(args.pressure_target_hpa))]
        steps.append(
            _run_step(
                name="dry_collect",
                output_dir=dry_dir,
                fn=validate_dry_collect.main,
                argv=dry_argv,
                write_device=False,
            )
        )
        if args.fail_fast and steps[-1]["status"] == "FAIL":
            payload = {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "config_path": str(Path(args.config).resolve()),
                "output_dir": str(base_output),
                "analyzers": _configured_analyzers(cfg),
                "include_pressure_only": bool(args.include_pressure),
                "include_roundtrip": bool(args.include_roundtrip),
                "allow_write_back_same": bool(args.allow_write_back_same),
                "steps": steps,
                "overall_status": _overall_status(steps),
            }
            _write_summary_files(base_output, payload)
            return 1

    if args.include_roundtrip:
        roundtrip_dir = base_output / "roundtrip"
        roundtrip_argv = [
            "--config",
            str(Path(args.config).resolve()),
            "--output-dir",
            str(roundtrip_dir),
        ]
        if args.analyzer:
            roundtrip_argv += ["--analyzer", str(args.analyzer)]
        if args.allow_write_back_same:
            roundtrip_argv.append("--write-back-same")
        steps.append(
            _run_step(
                name="roundtrip",
                output_dir=roundtrip_dir,
                fn=verify_coefficient_roundtrip.main,
                argv=roundtrip_argv,
                write_device=bool(args.allow_write_back_same),
            )
        )
        if args.fail_fast and steps[-1]["status"] == "FAIL":
            payload = {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "config_path": str(Path(args.config).resolve()),
                "output_dir": str(base_output),
                "analyzers": _configured_analyzers(cfg),
                "include_pressure_only": bool(args.include_pressure),
                "include_roundtrip": True,
                "allow_write_back_same": bool(args.allow_write_back_same),
                "steps": steps,
                "overall_status": _overall_status(steps),
            }
            _write_summary_files(base_output, payload)
            return 1

    if args.include_pressure:
        pressure_dir = base_output / "pressure_only"
        pressure_argv = [
            "--config",
            str(Path(args.config).resolve()),
            "--output-dir",
            str(pressure_dir),
            "--pressure-points",
            str(args.pressure_points),
        ]
        if args.count is not None:
            pressure_argv += ["--count", str(int(args.count))]
        if args.interval_s is not None:
            pressure_argv += ["--interval-s", str(float(args.interval_s))]
        if args.no_prompt:
            pressure_argv.append("--no-prompt")
        steps.append(
            _run_step(
                name="pressure_only",
                output_dir=pressure_dir,
                fn=validate_pressure_only.main,
                argv=pressure_argv,
                write_device=False,
            )
        )

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config_path": str(Path(args.config).resolve()),
        "output_dir": str(base_output),
        "analyzers": _configured_analyzers(cfg),
        "include_pressure_only": bool(args.include_pressure),
        "include_roundtrip": bool(args.include_roundtrip),
        "allow_write_back_same": bool(args.allow_write_back_same),
        "offline_run_dir": str(Path(args.offline_run_dir).resolve()) if args.offline_run_dir else "",
        "steps": steps,
        "overall_status": _overall_status(steps),
    }
    outputs = _write_summary_files(base_output, payload)
    _log(f"[summary] {payload['overall_status']}")
    _log(f"[summary] json={outputs['summary_json']}")
    _log(f"[summary] md={outputs['summary_md']}")
    return 0 if payload["overall_status"] != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
