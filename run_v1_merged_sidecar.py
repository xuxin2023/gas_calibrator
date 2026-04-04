"""Root launcher for the V1 merged-calibration sidecar.

This provides a discoverable repository-level entrypoint while keeping the
workflow outside the frozen V1 production UI and default Step 2 path.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.run_v1_merged_calibration_sidecar import main as run_sidecar


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_sidecar(args))


if __name__ == "__main__":
    raise SystemExit(main())
