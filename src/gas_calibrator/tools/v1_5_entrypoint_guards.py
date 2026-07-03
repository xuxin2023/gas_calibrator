"""Shared guardrails for V1.5 non-formal tool entrypoints."""

from __future__ import annotations

import argparse
from typing import Any


DIAGNOSTIC_OPERATOR_CONFIRMATION = "DIAGNOSTIC_ONLY"


def add_engineering_diagnostic_guard_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--engineering-diagnostic",
        action="store_true",
        help="Required for diagnostic hardware probes. Confirms this is not a formal calibration entrypoint.",
    )
    parser.add_argument(
        "--not-real-acceptance",
        action="store_true",
        help="Required for diagnostic hardware probes. Confirms generated evidence is not formal acceptance.",
    )
    parser.add_argument(
        "--operator-confirmation",
        default="",
        help=f"Required value for diagnostic hardware probes: {DIAGNOSTIC_OPERATOR_CONFIRMATION}.",
    )


def require_engineering_diagnostic_guard(
    args: Any,
    parser: argparse.ArgumentParser,
    *,
    context: str,
) -> None:
    if (
        getattr(args, "engineering_diagnostic", False)
        and getattr(args, "not_real_acceptance", False)
        and str(getattr(args, "operator_confirmation", "")).strip() == DIAGNOSTIC_OPERATOR_CONFIRMATION
    ):
        return
    parser.error(
        f"{context} is an engineering diagnostic entrypoint, not the V1.5 formal route. "
        "Pass --engineering-diagnostic --not-real-acceptance "
        f"--operator-confirmation {DIAGNOSTIC_OPERATOR_CONFIRMATION}."
    )
