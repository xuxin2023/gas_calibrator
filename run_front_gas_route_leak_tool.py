"""Root launcher for the independent front gas-route leak tool."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.front_gas_route_leak_tool import main as run_front_gas_route_leak_tool


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_front_gas_route_leak_tool(args))


if __name__ == "__main__":
    raise SystemExit(main())
