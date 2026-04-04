"""
V2 离线筛选重算入口。

仅服务于离线建模分析，不参与在线自动校准运行流程。
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from ..config.offline_modeling import OfflineRefitConfig
from ..core.refit_filtering import export_refit_filtering_result, run_refit_filtering
from ..exceptions import ConfigurationInvalidError, DataParseError


def _log(message: str) -> None:
    print(message)


def _read_frame(input_path: Path, *, sheet_name: str = "0") -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(input_path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path, sheet_name=sheet_name if sheet_name else 0)
    raise ConfigurationInvalidError("input_path", str(input_path), reason="仅支持 csv/xlsx/xls")


def load_refit_config(config_path: Optional[str]) -> OfflineRefitConfig:
    if not config_path:
        return OfflineRefitConfig()
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationInvalidError("config_path", config_path, reason="配置文件不存在")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return OfflineRefitConfig.from_dict(payload)


def run_from_cli(
    *,
    input_path: str,
    gas_type: str,
    analyzer_id: Optional[str] = None,
    config_path: Optional[str] = None,
    sheet_name: str = "0",
    output_dir: Optional[str] = None,
) -> Dict[str, str]:
    """执行离线筛选重算。"""
    config = load_refit_config(config_path)
    config.gas_type = str(gas_type).strip().lower()
    config.enabled = True
    if output_dir:
        config.output_dir = output_dir
    source_path = Path(input_path)
    if not source_path.exists():
        raise ConfigurationInvalidError("input_path", input_path, reason="输入文件不存在")

    _log("离线筛选重算已启用")
    _log("该功能仅用于离线建模分析，不影响在线自动校准流程")
    _log(f"输入文件：{source_path}")
    _log(f"气体类型：{config.gas_type}")
    frame = _read_frame(source_path, sheet_name=sheet_name)
    rows = frame.to_dict(orient="records")

    result = run_refit_filtering(rows, config=config, analyzer_id=analyzer_id, log_fn=_log)
    run_dir = Path(config.output_dir).resolve() / f"refit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    exported = export_refit_filtering_result(
        result,
        run_dir,
        prefix=f"{analyzer_id or 'ALL'}_{config.gas_type}",
    )
    summary_payload = {
        "input_path": str(source_path),
        "gas_type": config.gas_type,
        "analyzer_id": analyzer_id,
        "exported_paths": {key: str(value) for key, value in exported.items()},
    }
    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"结果目录：{run_dir}")
    _log(f"筛选明细：{exported['audit_csv']}")
    _log(f"重拟合报告：{exported['excel']}")
    return {key: str(value) for key, value in exported.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="V2 离线点表筛选重算入口")
    parser.add_argument("--input", required=True, help="输入点表文件路径，支持 csv/xlsx/xls")
    parser.add_argument("--gas", required=True, choices=["co2", "h2o"], help="单气体表类型")
    parser.add_argument("--analyzer", help="单台分析仪编号，例如 GA01")
    parser.add_argument("--config", help="筛选重算配置 JSON 文件")
    parser.add_argument("--sheet", default="0", help="Excel sheet 名称或索引")
    parser.add_argument("--output-dir", help="输出目录")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        run_from_cli(
            input_path=args.input,
            gas_type=args.gas,
            analyzer_id=args.analyzer,
            config_path=args.config,
            sheet_name=args.sheet,
            output_dir=args.output_dir,
        )
    except Exception as exc:  # pragma: no cover
        raise DataParseError("offline_refit_runner", reason=str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
