from __future__ import annotations

from gas_calibrator.tools.front_gas_route_leak_tool import (
    CLI_LAUNCHER,
    FrontGasRouteLeakToolState,
    build_cli_arguments,
    build_cli_command,
)


def test_build_cli_arguments_includes_curve_ready_leak_check_defaults() -> None:
    state = FrontGasRouteLeakToolState(
        allow_live_hardware=True,
        configure_analyzer_stream=True,
        source_close_first_delay_s="3",
    )

    argv = build_cli_arguments(state, run_id="20260402_090000")

    assert "--allow-live-hardware" in argv
    assert "--configure-analyzer-stream" in argv
    assert "--source-close-first-delay-s" in argv
    assert "3" in argv
    assert "--run-id" in argv
    assert "20260402_090000" in argv


def test_build_cli_command_targets_root_leak_check_launcher() -> None:
    state = FrontGasRouteLeakToolState(allow_live_hardware=True)

    command = build_cli_command(state, run_id="20260402_090500")

    assert str(CLI_LAUNCHER) in command
    assert "--allow-live-hardware" in command
