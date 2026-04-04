"""离线建模分析启动入口。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.modeling.offline_model_runner import run_offline_modeling_analysis


def main() -> int:
    parser = argparse.ArgumentParser(description="运行离线建模分析，不影响在线自动校准流程。")
    parser.add_argument("--base-config", default=str(ROOT / "configs" / "default_config.json"))
    parser.add_argument("--modeling-config", default=str(ROOT / "configs" / "modeling_offline.json"))
    parser.add_argument("--input", default=None, help="可选：覆盖配置中的输入文件路径")
    args = parser.parse_args()

    try:
        result = run_offline_modeling_analysis(
            base_config_path=args.base_config,
            modeling_config_path=args.modeling_config,
            input_path=args.input,
            log_fn=print,
        )
    except Exception as exc:
        print(f"离线建模分析失败：{exc}")
        return 1

    print(f"离线建模分析完成：{result['run_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
