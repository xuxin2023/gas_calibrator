"""
桌面应用启动入口。

职责：
1. 把项目内 `src` 目录加入 `sys.path`，保证本地源码可直接导入；
2. 调用主 UI 入口函数并启动程序。
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
# 兼容“源码目录直接运行”场景，避免要求先安装成 site-package。
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.ui.app import main


if __name__ == "__main__":
    main()
