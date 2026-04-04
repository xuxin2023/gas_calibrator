import importlib
import sys

import pytest


def test_adapters_package_import_is_lazy() -> None:
    for name in [
        "gas_calibrator.v2.adapters",
        "gas_calibrator.v2.adapters.offline_refit_runner",
        "gas_calibrator.v2.adapters.v1_postprocess_runner",
        "gas_calibrator.v2.adapters.analyzer_coefficient_downloader",
    ]:
        sys.modules.pop(name, None)

    import gas_calibrator.v2.adapters as adapters

    assert "gas_calibrator.v2.adapters.offline_refit_runner" not in sys.modules
    assert "gas_calibrator.v2.adapters.v1_postprocess_runner" not in sys.modules

    downloader = adapters.download_coefficients_to_analyzers

    assert callable(downloader)
    assert "gas_calibrator.v2.adapters.analyzer_coefficient_downloader" in sys.modules
    assert "gas_calibrator.v2.adapters.offline_refit_runner" not in sys.modules
    assert "gas_calibrator.v2.adapters.v1_postprocess_runner" not in sys.modules


def test_analyzer_coefficient_downloader_module_import_stays_lightweight() -> None:
    for name in [
        "gas_calibrator.v2.adapters.analyzer_coefficient_downloader",
        "gas_calibrator.devices.gas_analyzer",
    ]:
        sys.modules.pop(name, None)

    module = importlib.import_module("gas_calibrator.v2.adapters.analyzer_coefficient_downloader")

    assert hasattr(module, "download_coefficients_to_analyzers")
    assert "gas_calibrator.devices.gas_analyzer" not in sys.modules


def test_analyzer_coefficient_downloader_raises_clear_error_on_execution_when_driver_dep_missing(
    monkeypatch,
    tmp_path,
) -> None:
    module = importlib.import_module("gas_calibrator.v2.adapters.analyzer_coefficient_downloader")

    monkeypatch.setattr(
        module,
        "load_download_plan",
        lambda path: [
            {
                "Analyzer": "GA01",
                "Gas": "CO2",
                "PrimaryCommand": "PRIMARY",
                "SecondaryCommand": "",
                "ModeEnterCommand": "MODE2",
                "ModeExitCommand": "MODE1",
            }
        ],
    )
    monkeypatch.setattr(
        module,
        "load_download_targets",
        lambda path: [module.AnalyzerDownloadTarget(analyzer="GA01", port="COM1")],
    )

    def _missing_dependency(module_name: str):
        raise ModuleNotFoundError("No module named 'serial'", name="serial")

    monkeypatch.setattr(module, "import_module", _missing_dependency)

    with pytest.raises(ImportError) as exc_info:
        module.download_coefficients_to_analyzers(
            report_path=tmp_path / "report.xlsx",
            config_path=tmp_path / "config.json",
            output_dir=tmp_path / "out",
        )

    message = str(exc_info.value)
    assert "serial" in message
    assert "real analyzer coefficient download" in message
