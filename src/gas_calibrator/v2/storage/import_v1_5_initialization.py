"""Compatibility CLI for V1.5 initialization database import.

Prefer ``gas_calibrator.v1_5.import_initialization_database`` for new V1.5
flows. This historical V2 storage path remains as a wrapper for existing tests
and scripts.
"""

from __future__ import annotations

from ...v1_5.import_initialization_database import main, run_import


__all__ = ["main", "run_import"]


if __name__ == "__main__":
    raise SystemExit(main())
