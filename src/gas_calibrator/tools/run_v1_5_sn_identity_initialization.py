"""Compatibility CLI for V1.5 SN identity initialization.

The implementation lives in ``gas_calibrator.v1_5.sn_identity_initialization``
so first-discovery SN work stays in the V1.5 pre-route layer. This wrapper keeps
the historical command path working.
"""

from __future__ import annotations

from ..v1_5.sn_identity_initialization import (
    AUTHORIZATION_PHRASE,
    DEFAULT_OUTPUT_ROOT,
    SN_RE,
    PROTOCOL_ID_RE,
    AnalyzerFactory,
    SnIdentityInitializationError,
    build_sn_identity_initialization_plan,
    execute_sn_identity_initialization,
    main,
)


__all__ = [
    "AUTHORIZATION_PHRASE",
    "DEFAULT_OUTPUT_ROOT",
    "SN_RE",
    "PROTOCOL_ID_RE",
    "AnalyzerFactory",
    "SnIdentityInitializationError",
    "build_sn_identity_initialization_plan",
    "execute_sn_identity_initialization",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
