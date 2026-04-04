"""Root launcher for the independent metrology seal/pressure qualification diagnostic V2."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic import main as run_room_temp_co2_pressure_diagnostic


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_room_temp_co2_pressure_diagnostic(args))


if __name__ == "__main__":
    raise SystemExit(main())
