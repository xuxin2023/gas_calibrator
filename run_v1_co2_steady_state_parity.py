"""Root launcher for the V1 CO2 steady-state replay/parity audit sidecar.

This keeps the audit flow easy to discover without wiring it into the V1 UI or
changing the default ``run_app.py`` behavior.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.tools.audit_v1_co2_steady_state_parity import main as run_audit


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    return int(run_audit(args))


if __name__ == "__main__":
    raise SystemExit(main())
