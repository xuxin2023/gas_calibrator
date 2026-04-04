"""Root launcher for the independent CO2 raw-ratio gas-route leak check."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.run_gas_route_ratio_leak_check import main as run_leak_check


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_leak_check(args))


if __name__ == "__main__":
    raise SystemExit(main())
