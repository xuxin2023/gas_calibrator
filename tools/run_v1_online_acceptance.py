from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from gas_calibrator.tools.run_v1_online_acceptance import main


if __name__ == "__main__":
    raise SystemExit(main())
