import argparse

import pytest

from gas_calibrator.tools import (
    run_v1_5_open_flow_dynamic_pressure_diagnostic as dynamic_pressure,
    run_v1_5_pace_mode_ingress_diagnostic as pace_ingress,
    run_v1_5_sealed_pressure_tune_900 as sealed_tune,
)
from gas_calibrator.tools.v1_5_entrypoint_guards import (
    DIAGNOSTIC_OPERATOR_CONFIRMATION,
    add_engineering_diagnostic_guard_args,
    require_engineering_diagnostic_guard,
)


pytestmark = pytest.mark.v1_5_formal_gate


def test_engineering_diagnostic_guard_requires_all_three_confirmations() -> None:
    parser = argparse.ArgumentParser()
    add_engineering_diagnostic_guard_args(parser)

    missing = parser.parse_args(["--engineering-diagnostic", "--not-real-acceptance"])
    with pytest.raises(SystemExit):
        require_engineering_diagnostic_guard(missing, parser, context="test diagnostic")

    ok = parser.parse_args(
        [
            "--engineering-diagnostic",
            "--not-real-acceptance",
            "--operator-confirmation",
            DIAGNOSTIC_OPERATOR_CONFIRMATION,
        ]
    )
    require_engineering_diagnostic_guard(ok, parser, context="test diagnostic")


def test_dynamic_pressure_real_com_requires_diagnostic_guard() -> None:
    with pytest.raises(SystemExit):
        dynamic_pressure.main(["--real-com", "--gas-ppm", "0"])


def test_pace_ingress_real_com_requires_diagnostic_guard() -> None:
    with pytest.raises(SystemExit):
        pace_ingress.main(
            [
                "--real-com",
                "--i-understand-pressure-only-no-write",
                "--operator-confirm-sealed-volume",
            ]
        )


def test_sealed_pressure_tune_requires_diagnostic_guard_before_config_load(tmp_path) -> None:
    cfg = tmp_path / "missing.json"
    with pytest.raises(SystemExit):
        sealed_tune.main(["--config", str(cfg), "--confirm-pressure-only-tuning"])
