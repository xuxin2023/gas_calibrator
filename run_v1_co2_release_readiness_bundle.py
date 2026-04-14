"""Root launcher for the V1 CO2 release readiness evidence merge bundle sidecar."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.build_v1_co2_release_readiness_bundle import main as run_sidecar


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_sidecar(args))


if __name__ == "__main__":
    raise SystemExit(main())
