from gas_calibrator.v2.scripts import verify_v1_v2_skip0_replacement


def test_verify_v1_v2_skip0_replacement_builds_standard_skip0_argv() -> None:
    argv = verify_v1_v2_skip0_replacement.build_skip0_replacement_argv(["--simulation"])

    assert argv[:2] == ["--replacement-skip0", "--skip-connect-check"]
    assert argv[-1] == "--simulation"


def test_verify_v1_v2_skip0_replacement_forwards_expected_defaults(monkeypatch) -> None:
    captured = {}

    def _fake_main(argv=None):
        captured["argv"] = list(argv or [])
        return 0

    monkeypatch.setattr(
        verify_v1_v2_skip0_replacement.compare_v1_v2_control_flow,
        "main",
        _fake_main,
    )

    result = verify_v1_v2_skip0_replacement.main(["--simulation", "--run-name", "skip0_demo"])

    assert result == 0
    assert captured["argv"][:2] == ["--replacement-skip0", "--skip-connect-check"]
    assert "--simulation" in captured["argv"]
    assert "skip0_demo" in captured["argv"]
