from __future__ import annotations

import sys
from typing import Iterable, Optional

from . import compare_v1_v2_control_flow


DEFAULT_ARGS = ("--replacement-skip0-co2-only-diagnostic-relaxed", "--skip-connect-check")


def build_skip0_co2_only_diagnostic_relaxed_argv(argv: Optional[Iterable[str]] = None) -> list[str]:
    return [*DEFAULT_ARGS, *list(argv or [])]


def main(argv: Optional[Iterable[str]] = None) -> int:
    return compare_v1_v2_control_flow.main(build_skip0_co2_only_diagnostic_relaxed_argv(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
