from __future__ import annotations

import sys

from gas_calibrator.tools.audit_v1_co2_threshold_matrix import main as run_threshold_matrix


def main(argv: list[str] | None = None) -> int:
    return int(run_threshold_matrix(list(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
