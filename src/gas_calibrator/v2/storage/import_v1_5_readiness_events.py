"""Compatibility exports for the native V1.5 readiness event importer."""

from gas_calibrator.v1_5.import_readiness_events import main, run_import
from gas_calibrator.v1_5.readiness_event_database import (
    EVENT_TYPE,
    build_v1_5_readiness_event_preview,
    import_v1_5_readiness_events,
)

__all__ = [
    "EVENT_TYPE",
    "build_v1_5_readiness_event_preview",
    "import_v1_5_readiness_events",
    "main",
    "run_import",
]


if __name__ == "__main__":
    raise SystemExit(main())
