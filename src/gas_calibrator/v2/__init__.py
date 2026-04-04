"""Gas Calibrator V2 package.

Keep top-level imports light so compare/report/UI tests can collect without
pulling the full runtime stack at import time.
"""

from __future__ import annotations

from typing import Any

__version__ = "2.0.0-alpha"


def create_calibration_service(*args: Any, **kwargs: Any) -> Any:
    from .entry import create_calibration_service as _impl

    return _impl(*args, **kwargs)


def run_calibration(*args: Any, **kwargs: Any) -> Any:
    from .entry import run_calibration as _impl

    return _impl(*args, **kwargs)


__all__ = [
    "create_calibration_service",
    "run_calibration",
]
