from __future__ import annotations

import sys
from pathlib import Path

import pytest


SRC_ROOT = Path(__file__).resolve().parents[3]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


@pytest.fixture
def sample_run_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / "output" / "test_v2_safe" / "run_20260322_223711"
