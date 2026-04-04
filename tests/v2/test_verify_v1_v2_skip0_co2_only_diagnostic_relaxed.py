from gas_calibrator.v2.scripts import verify_v1_v2_skip0_co2_only_diagnostic_relaxed


def test_verify_v1_v2_skip0_co2_only_diagnostic_relaxed_builds_standard_argv() -> None:
    argv = verify_v1_v2_skip0_co2_only_diagnostic_relaxed.build_skip0_co2_only_diagnostic_relaxed_argv(
        ["--simulation"]
    )

    assert argv[:2] == [
        "--replacement-skip0-co2-only-diagnostic-relaxed",
        "--skip-connect-check",
    ]
    assert argv[-1] == "--simulation"


def test_verify_v1_v2_skip0_co2_only_diagnostic_relaxed_forwards_expected_defaults(monkeypatch) -> None:
    captured = {}

    def _fake_main(argv=None):
        captured["argv"] = list(argv or [])
        return 0

    monkeypatch.setattr(
        verify_v1_v2_skip0_co2_only_diagnostic_relaxed.compare_v1_v2_control_flow,
        "main",
        _fake_main,
    )

    result = verify_v1_v2_skip0_co2_only_diagnostic_relaxed.main(
        ["--simulation", "--run-name", "diag_skip0_co2_only_demo"]
    )

    assert result == 0
    assert captured["argv"][:2] == [
        "--replacement-skip0-co2-only-diagnostic-relaxed",
        "--skip-connect-check",
    ]
    assert "--simulation" in captured["argv"]
    assert "diag_skip0_co2_only_demo" in captured["argv"]
