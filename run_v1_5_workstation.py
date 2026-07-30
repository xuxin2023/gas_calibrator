"""Launch the final-product V1.5 dry-run operator workstation.

This explicit entry does not replace ``run_app.py`` or the V1 fallback.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.v1_5.ui.operator_workstation_app import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
