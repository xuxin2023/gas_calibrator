from __future__ import annotations

import sys
from typing import Iterable, Optional

from . import compare_v1_v2_control_flow


DEFAULT_ARGS = ("--replacement-skip0", "--skip-connect-check")


def build_skip0_replacement_argv(argv: Optional[Iterable[str]] = None) -> list[str]:
    return [*DEFAULT_ARGS, *list(argv or [])]


def main(argv: Optional[Iterable[str]] = None) -> int:
    return compare_v1_v2_control_flow.main(build_skip0_replacement_argv(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
