from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, file_name: str):
    path = REPO_ROOT / file_name
    spec = spec_from_file_location(module_name, path)
    module = module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_v1_postprocess_delegates_to_gui(monkeypatch) -> None:
    module = _load_module("test_run_v1_postprocess_entry", "run_v1_postprocess.py")
    calls: list[str] = []

    monkeypatch.setattr(module, "launch_gui", lambda: calls.append("gui") or 0)

    result = module.main([])

    assert result == 0
    assert calls == ["gui"]


def test_run_v1_postprocess_rejects_cli_args() -> None:
    module = _load_module("test_run_v1_postprocess_entry_args", "run_v1_postprocess.py")

    try:
        module.main(["--unexpected"])
    except SystemExit as exc:
        assert str(exc) == "run_v1_postprocess.py does not accept CLI arguments"
    else:  # pragma: no cover
        raise AssertionError("expected CLI argument validation error")


def test_run_v1_merged_sidecar_delegates_arguments(monkeypatch) -> None:
    module = _load_module("test_run_v1_merged_sidecar_entry", "run_v1_merged_sidecar.py")
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_sidecar", _fake_main)

    result = module.main(["--run-dir", r"D:\logs\run_20260329_120000"])

    assert result == 0
    assert captured["argv"] == ["--run-dir", r"D:\logs\run_20260329_120000"]


def test_run_v1_co2_calibration_candidate_pack_delegates_arguments(monkeypatch) -> None:
    module = _load_module(
        "test_run_v1_co2_calibration_candidate_pack_entry",
        "run_v1_co2_calibration_candidate_pack.py",
    )
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_sidecar", _fake_main)

    result = module.main(
        [
            "--points-csv",
            r"D:\logs\points_demo.csv",
            "--output-dir",
            r"D:\logs\pack_out",
        ]
    )

    assert result == 0
    assert captured["argv"] == [
        "--points-csv",
        r"D:\logs\points_demo.csv",
        "--output-dir",
        r"D:\logs\pack_out",
    ]


def test_run_v1_co2_weighted_fit_advisory_delegates_arguments(monkeypatch) -> None:
    module = _load_module(
        "test_run_v1_co2_weighted_fit_advisory_entry",
        "run_v1_co2_weighted_fit_advisory.py",
    )
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_sidecar", _fake_main)

    result = module.main(
        [
            "--candidate-pack-json",
            r"D:\logs\candidate_pack.json",
            "--output-dir",
            r"D:\logs\fit_out",
        ]
    )

    assert result == 0
    assert captured["argv"] == [
        "--candidate-pack-json",
        r"D:\logs\candidate_pack.json",
        "--output-dir",
        r"D:\logs\fit_out",
    ]


def test_run_v1_co2_fit_stability_audit_delegates_arguments(monkeypatch) -> None:
    module = _load_module(
        "test_run_v1_co2_fit_stability_audit_entry",
        "run_v1_co2_fit_stability_audit.py",
    )
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_sidecar", _fake_main)

    result = module.main(
        [
            "--weighted-fit-summary-json",
            r"D:\logs\weighted_fit_summary.json",
            "--output-dir",
            r"D:\logs\stability_out",
        ]
    )

    assert result == 0
    assert captured["argv"] == [
        "--weighted-fit-summary-json",
        r"D:\logs\weighted_fit_summary.json",
        "--output-dir",
        r"D:\logs\stability_out",
    ]


def test_run_v1_co2_bootstrap_robustness_audit_delegates_arguments(monkeypatch) -> None:
    module = _load_module(
        "test_run_v1_co2_bootstrap_robustness_audit_entry",
        "run_v1_co2_bootstrap_robustness_audit.py",
    )
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_sidecar", _fake_main)

    result = module.main(
        [
            "--weighted-fit-summary-json",
            r"D:\logs\weighted_fit_summary.json",
            "--output-dir",
            r"D:\logs\bootstrap_out",
        ]
    )

    assert result == 0
    assert captured["argv"] == [
        "--weighted-fit-summary-json",
        r"D:\logs\weighted_fit_summary.json",
        "--output-dir",
        r"D:\logs\bootstrap_out",
    ]
