from pathlib import Path

from gas_calibrator.v2.calibration.temperature_compensation import fit_temperature_compensation, format_senco_coeffs
from gas_calibrator.v2.export.temperature_compensation_export import export_temperature_compensation_artifacts


def test_fit_temperature_compensation_returns_coefficients():
    result = fit_temperature_compensation([10, 20, 30, 40], [11, 21, 31, 41], polynomial_order=3)
    assert result["fit_ok"] is True
    assert result["n_points"] == 4
    formatted = format_senco_coeffs((result["A"], result["B"], result["C"], result["D"]))
    assert len(formatted) == 4
    assert all("e+" not in item for item in formatted)
    assert all(item.count("e") == 1 for item in formatted)


def test_export_temperature_compensation_artifacts_writes_files(tmp_path: Path):
    payload = export_temperature_compensation_artifacts(
        tmp_path,
        [
            {
                "analyzer_id": "ga01",
                "ref_temp_c": 25.0,
                "ref_temp_source": "env",
                "cell_temp_raw_c": 24.8,
                "shell_temp_raw_c": 25.4,
                "valid_for_cell_fit": True,
                "valid_for_shell_fit": True,
            },
            {
                "analyzer_id": "ga01",
                "ref_temp_c": 35.0,
                "ref_temp_source": "env",
                "cell_temp_raw_c": 34.7,
                "shell_temp_raw_c": 35.5,
                "valid_for_cell_fit": True,
                "valid_for_shell_fit": True,
            },
        ],
    )
    assert payload["results"]
    assert payload["paths"]["observations_csv"].exists()
    assert payload["paths"]["results_csv"].exists()
    assert payload["paths"]["commands_txt"].exists()
    assert payload["paths"]["workbook"].exists()
    assert payload["results"][0]["command_string"].startswith("SENCO7,YGAS,FFF,")
    assert "e+" not in payload["results"][0]["command_string"]
