"""Build a no-write H2O special diagnostic queue for V1.5.

The queue generated here is an offline planning artifact. It does not open COM
ports, does not operate valves, and does not write SENCO coefficients. It turns
an H2O state-transfer block decision into a small, executable diagnostic plan
that can later be run through the proven V1.5 H2O open-flow queue runner.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class H2OSpecialDiagnosticQueueInputs:
    device_decision_csv: Path
    target_device_id: str = "084"
    temp_c: float = 20.0
    hgen_temp_c: float = 20.0
    low_rh_pct: float = 30.0
    high_rh_pct: float = 70.0
    purge_s: float = 360.0
    sample_count: int = 30
    sample_interval_s: float = 1.0
    analyzer_acquisition: str = "active_stream_1hz"
    reference_pressure_hpa: float = 1013.25
    config_placeholder: str = "<V1_5_RUNTIME_CONFIG_JSON>"
    run_output_placeholder: str = "<H2O_084_SPECIAL_DIAGNOSTIC_OUTPUT_DIR>"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _float(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    return path


def _format_number(value: float, digits: int = 6) -> str:
    text = f"{float(value):.{digits}f}".rstrip("0").rstrip(".")
    return text if text else "0"


def _saturation_vapor_pressure_hpa(temp_c: float) -> float:
    # Magnus formula over water, adequate for diagnostic planning references.
    return 6.112 * math.exp((17.62 * float(temp_c)) / (243.12 + float(temp_c)))


def _dewpoint_from_vapor_pressure_hpa(vapor_pressure_hpa: float) -> float:
    if vapor_pressure_hpa <= 0:
        return float("nan")
    gamma = math.log(vapor_pressure_hpa / 6.112)
    return (243.12 * gamma) / (17.62 - gamma)


def _humidity_reference(*, temp_c: float, rh_pct: float, pressure_hpa: float) -> Dict[str, float]:
    vapor_pressure = _saturation_vapor_pressure_hpa(temp_c) * max(float(rh_pct), 0.0) / 100.0
    return {
        "reference_dewpoint_c": _dewpoint_from_vapor_pressure_hpa(vapor_pressure),
        "reference_h2o_mmol": vapor_pressure / float(pressure_hpa) * 1000.0,
        "water_vapor_pressure_hpa": vapor_pressure,
    }


def _decision_for_device(path: Path, target_device_id: str) -> Mapping[str, Any]:
    target = _device_id(target_device_id)
    for row in _read_csv(path):
        if _device_id(row.get("device_id") or row.get("analyzer_device_id")) == target:
            return row
    return {}


def _point_id(*, index: int, target_device_id: str, temp_c: float, hgen_temp_c: float, rh_pct: float, suffix: str) -> str:
    temp_token = _format_number(temp_c).replace("-", "m").replace(".", "p")
    hgen_token = _format_number(hgen_temp_c).replace("-", "m").replace(".", "p")
    rh_token = _format_number(rh_pct).replace("-", "m").replace(".", "p")
    return f"d{_device_id(target_device_id)}_diag_p{index:03d}_T{temp_token}_HG{hgen_token}C_{rh_token}RH_{suffix}"


def build_h2o_special_diagnostic_queue_tables(
    inputs: H2OSpecialDiagnosticQueueInputs,
) -> Dict[str, List[Dict[str, Any]]]:
    target = _device_id(inputs.target_device_id)
    decision_row = _decision_for_device(Path(inputs.device_decision_csv), target)
    source_decision = str(decision_row.get("state_transfer_decision") or "unknown")
    source_blockers = str(decision_row.get("blockers") or "")

    execution_plan: List[Dict[str, Any]] = [
        {
            "sequence": 1,
            "device_id": target,
            "phase": "identity_snapshot",
            "runner_queue_point": False,
            "action": "读取设备ID并备份GETCO2/4/6/7/8、S6、状态寄存器，确认当前串口确实是目标设备。",
            "physical_meaning": "先锁定身份和旧系数，避免把串口漂移或旧输出层修正误当作水路响应问题。",
            "required_evidence": "device_id; GETCO2; GETCO4; GETCO6; GETCO7; GETCO8; status_register",
            "write_senco_allowed": False,
        },
        {
            "sequence": 2,
            "device_id": target,
            "phase": "dry_gas_low_water_anchor",
            "runner_queue_point": False,
            "action": "如现场具备干气/N2通道，先做低水锚点开放流通采样；若不能自动切干气，则至少记录跳过原因。",
            "physical_meaning": "干气点约束H2O低端响应；它不是CO2零气点，必须由露点/参考水分证据证明低水状态。",
            "required_evidence": "dewpoint; h2o_ratio_filtered; h2o_ratio_raw; ref_signal; h2o_signal; chamber_temp; pressure",
            "write_senco_allowed": False,
        },
    ]

    point_specs = [
        ("low_humidity_a", "低湿点A，用于检查主H2O ratio与露点换算H2O是否同向。", inputs.low_rh_pct),
        ("high_humidity", "高湿点，用于检查H2O signal/ref_signal/状态寄存器是否接近饱和或异常。", inputs.high_rh_pct),
        ("low_humidity_return", "回到低湿点，检查同一物理点返回后S2/S4原始响应是否可重复。", inputs.low_rh_pct),
    ]
    runner_queue: List[Dict[str, Any]] = []
    for offset, (suffix, action, rh_pct) in enumerate(point_specs, start=3):
        reference = _humidity_reference(
            temp_c=float(inputs.hgen_temp_c),
            rh_pct=float(rh_pct),
            pressure_hpa=float(inputs.reference_pressure_hpa),
        )
        point_id = _point_id(
            index=offset - 2,
            target_device_id=target,
            temp_c=float(inputs.temp_c),
            hgen_temp_c=float(inputs.hgen_temp_c),
            rh_pct=float(rh_pct),
            suffix=suffix,
        )
        queue_row = {
            "point_id": point_id,
            "component": "h2o",
            "temp_c": float(inputs.temp_c),
            "hgen_temp_c": float(inputs.hgen_temp_c),
            "hgen_rh_pct": float(rh_pct),
            "reference_dewpoint_c": reference["reference_dewpoint_c"],
            "reference_h2o_mmol": reference["reference_h2o_mmol"],
            "certificate_uncertainty_mmol": "",
            "sample_role": "diagnostic",
            "purge_s": float(inputs.purge_s),
            "sample_count": int(inputs.sample_count),
            "analyzer_acquisition": str(inputs.analyzer_acquisition),
            "diagnostic_target_device_id": target,
            "diagnostic_phase": suffix,
            "writes_senco": False,
            "writes_device_id": False,
            "pressure_presample_policy": "skip",
            "required_signals": "H2O ratio filtered/raw; ref_signal; h2o_signal; status_register; chamber_temp; dewpoint; pressure",
        }
        runner_queue.append(queue_row)
        execution_plan.append(
            {
                "sequence": offset,
                "device_id": target,
                "phase": suffix,
                "runner_queue_point": True,
                "point_id": point_id,
                "action": action,
                "physical_meaning": "保持开放流通，让露点仪参考水分和分析仪H2O底层ratio在同一物理状态下比较。",
                "required_evidence": queue_row["required_signals"],
                "write_senco_allowed": False,
            }
        )

    command = _queue_command(inputs=inputs, queue_csv_placeholder="h2o_084_special_diagnostic_runner_queue.csv")
    summary = [
        {
            "device_id": target,
            "source_state_transfer_decision": source_decision,
            "source_blockers": source_blockers,
            "runner_queue_points": len(runner_queue),
            "opens_com_ports_now": False,
            "controls_routes_now": False,
            "writes_senco_now": False,
            "generated_for_later_real_run": True,
            "recommended_runner": "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
            "recommended_command": command,
            "physical_boundary": (
                "This artifact only plans a no-write diagnostic. The later real run must sample while "
                "the H2O route is open and must not treat diagnostic points as formal fit points until reviewed."
            ),
        }
    ]
    return {
        "h2o_special_diagnostic_summary": summary,
        "h2o_special_diagnostic_execution_plan": execution_plan,
        "h2o_special_diagnostic_runner_queue": runner_queue,
    }


def _queue_command(*, inputs: H2OSpecialDiagnosticQueueInputs, queue_csv_placeholder: str) -> str:
    return " ".join(
        [
            "python",
            "-m",
            "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
            "--config",
            str(inputs.config_placeholder),
            "--queue-csv",
            queue_csv_placeholder,
            "--output-dir",
            str(inputs.run_output_placeholder),
            "--run-id",
            f"h2o_{_device_id(inputs.target_device_id)}_special_diagnostic",
            "--temps",
            "all",
            "--temperature-order",
            "queue",
            "--no-control-temperature",
            "--purge-s",
            _format_number(inputs.purge_s),
            "--sample-count",
            str(int(inputs.sample_count)),
            "--sample-interval-s",
            _format_number(inputs.sample_interval_s),
            "--sensor-read-interval-s",
            "1",
            "--analyzer-acquisition",
            str(inputs.analyzer_acquisition),
            "--min-valid-analyzers",
            "1",
            "--h2o-pressure-presample-policy",
            "skip",
            "--no-ftd-write",
            "--no-prompt",
        ]
    )


def write_h2o_special_diagnostic_queue(
    *, inputs: H2OSpecialDiagnosticQueueInputs, output_dir: str | Path
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_special_diagnostic_queue_tables(inputs)
    outputs: Dict[str, Path] = {}
    filename_by_table = {
        "h2o_special_diagnostic_summary": "h2o_084_special_diagnostic_summary.csv",
        "h2o_special_diagnostic_execution_plan": "h2o_084_special_diagnostic_execution_plan.csv",
        "h2o_special_diagnostic_runner_queue": "h2o_084_special_diagnostic_runner_queue.csv",
    }
    for name, rows in tables.items():
        outputs[name] = _write_csv(output / filename_by_table[name], rows)
    outputs["runbook"] = _write_runbook(output / "h2o_084_special_diagnostic_runbook_zh.md", inputs, tables)
    meta = {
        "tool": "h2o_special_diagnostic_queue",
        "created_at": _now(),
        "inputs": {
            "device_decision_csv": str(Path(inputs.device_decision_csv).resolve()),
            "target_device_id": _device_id(inputs.target_device_id),
            "temp_c": float(inputs.temp_c),
            "hgen_temp_c": float(inputs.hgen_temp_c),
            "low_rh_pct": float(inputs.low_rh_pct),
            "high_rh_pct": float(inputs.high_rh_pct),
            "purge_s": float(inputs.purge_s),
            "sample_count": int(inputs.sample_count),
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "runner_queue_sample_role": "diagnostic",
        },
        "artifacts": {name: str(path) for name, path in outputs.items()},
    }
    meta_path = output / "h2o_084_special_diagnostic_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    outputs["meta"] = meta_path
    return outputs


def _write_runbook(
    path: Path,
    inputs: H2OSpecialDiagnosticQueueInputs,
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Path:
    summary = list(tables.get("h2o_special_diagnostic_summary", []))[0]
    command = str(summary.get("recommended_command") or "")
    target = _device_id(inputs.target_device_id)
    lines = [
        "# V1.5 084 水路专项 no-write 诊断队列",
        "",
        "## 目的",
        "",
        f"- 目标设备：`{target}`。",
        "- 目的：验证 H2O 主响应曲面在干气/低湿/高湿/返回点之间是否可迁移。",
        "- 这不是正式拟合队列，`sample_role=diagnostic`，跑完后必须再评审哪些点能进入正式水路拟合。",
        "",
        "## 边界",
        "",
        "- 本工具只生成离线队列和说明，不打开 COM。",
        "- 不写 `SENCO2/SENCO4/SENCO6`，不写设备 ID。",
        "- 复核时必须保证采样窗口发生在水路开放流通状态下。",
        "- 压力在开放流通水路中作为诊断输入记录，默认不作为采样前硬阻断。",
        "",
        "## 推荐执行命令",
        "",
        "```powershell",
        command,
        "```",
        "",
        "运行前把 `<V1_5_RUNTIME_CONFIG_JSON>` 和 `<H2O_084_SPECIAL_DIAGNOSTIC_OUTPUT_DIR>` 替换为现场配置与输出目录。",
        "",
        "## 关键判定",
        "",
        "- 若低湿 A 与低湿返回点的参考露点接近，但 H2O ratio 或 S2/S4 原始回放差异仍大，优先判定为 084 水路响应不可迁移。",
        "- 若高湿点出现 H2O signal、ref_signal 或状态寄存器异常，应先定位光学/信号链问题，不用 S6 输出层修正硬压。",
        "- 干气低水锚点只约束 H2O 低端；它不能和 CO2 零气点混为同一个锚点。",
        "",
        "## 输出文件",
        "",
        "- `h2o_084_special_diagnostic_runner_queue.csv`：可交给 H2O open-flow queue runner 的湿度诊断点。",
        "- `h2o_084_special_diagnostic_execution_plan.csv`：包含身份快照、干气低水锚和湿度点的完整诊断计划。",
        "- `h2o_084_special_diagnostic_summary.csv`：诊断边界和推荐命令摘要。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
