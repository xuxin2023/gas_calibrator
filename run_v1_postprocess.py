"""Root launcher for the V1 offline postprocess sidecar.

This keeps the entrypoint easy to discover without wiring the workflow back
into the V1 production UI or changing the default ``run_app.py`` path.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gas_calibrator.v2.scripts.v1_postprocess_gui import main as launch_gui


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    if args:
        raise SystemExit("run_v1_postprocess.py does not accept CLI arguments")
    return int(launch_gui())


if __name__ == "__main__":
    raise SystemExit(main())
